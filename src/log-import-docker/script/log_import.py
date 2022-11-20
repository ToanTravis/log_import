# -*- coding: utf-8 -*-

import argparse
import csv
import datetime
import gzip
import logging
import os
import re
import sys
import yaml
import multiprocessing
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as arcsv

import boto3

from botocore.errorfactory import ClientError
from utils import bulk_import
from utils import td
from utils.ecs import task_exists
from utils.sns import publish_err_message

stdout_handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter("%(levelname)s - %(message)s")
stdout_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stdout_handler)


class LogImporterInitializer(object):
    """
    Class for initialize log files before importing.
    """
    def __init__(self, platform, log_type, log_files_limit, run_env, version=0):
        with open('config.yml', 'r') as yml:
            self.config = yaml.load(yml, Loader=yaml.SafeLoader)[run_env]
        self.platform = platform
        self.log_type = log_type
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = self.config['bucket_name']
        self.s3_bucket = self.s3_resource.Bucket(self.bucket_name)
        self.version = int(version)
        self.num_parallel = self.config[self.platform][self.log_type]["num_parallel"]
        self.log_files_limit = log_files_limit
        self.init_s3_import_files()

        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)

    def init_s3_import_files(self):
        """
        get latest or specified version and metadata
        """
        path = 'data/{}/metadata/'.format(self.platform)
        if self.version != 0:
            version = 'v{}'.format(self.version)
            metapath = path + 'v{}/{}'.format(self.version, self.log_type)
            try:
                metadata = self.s3_resource.Object(
                    self.bucket_name, metapath
                ).get()['Body'].read().decode('utf-8').splitlines()
            except Exception:
                message = 'Specified version does not exist [{}]'.format(metapath)
                logger.error(message)
                raise RuntimeError(message)
            columns = {'time': 'int'}
            for column in metadata:
                column_name, column_type = column.split(':')
                columns[column_name] = column_type
            self.columns = columns
        else:
            versions = self.s3_bucket.objects.filter(Prefix=path)
            latest_version = 0
            latest_metapath = ''
            for vpath in versions:
                vpath_list = vpath.key.split('/', 5)
                if len(vpath_list) >= 4:
                    metapath = '/'.join(vpath_list[:4])
                    logger.info('metapath: ' + metapath)
                else:
                    metapath = ''
                version = re.sub('[^0-9]', '', vpath.key)
                if version == '':
                    continue
                if int(version) >= int(latest_version):
                    latest_version = version
                    latest_metapath = metapath

            metapaths = []
            for obj in self.s3_bucket.objects.filter(Prefix=latest_metapath):
                metapaths.append(obj.key)
            # check metadata
            for metapath in metapaths:
                sp = metapath.split('/')
                log_type = sp[4]
                if log_type != self.log_type:
                    continue
                metadata = self.s3_resource.Object(
                    self.bucket_name, metapath
                ).get()['Body'].read().decode('utf-8').splitlines()
                columns = {'time': 'int'}
                for column in metadata:
                    column_name, column_type = column.split(':')
                    columns[column_name] = column_type
                self.columns = columns
                self.version = latest_version
                break

    def run(self, start_date, end_date):
        """
        Import logs using td import commands
        """
        # 取り込み対象取得

        start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        date_target_files = []

        for date in daterange(start, end):
            renamed_s3_files = []

            str_date = date.strftime('%Y-%m-%d')
            logger.info('target date: ' + str_date)
            prefix = '/'.join(
                ['data', self.platform, self.log_type, str_date, '2']
            )
            objs = self.s3_bucket.objects.filter(Prefix=prefix)
            for obj in objs:
                if not self.validate_version(obj):
                    continue
                if obj.size < 128:
                    logger.warning('Target file is too small.')
                logger.info(obj)

                # 取り込みファイル数を制限する
                if (len(renamed_s3_files) >= self.log_files_limit):
                    logger.info(
                        "Log files number is exceeded {}. Skip adding log file".format(
                            self.log_files_limit
                        )
                    )
                    break

                # 取り込み中のファイル名にリネーム
                renamed_s3_filepath = self.rename_s3_file_to_importing(obj.key)
                renamed_s3_files.append(renamed_s3_filepath)

            # 取り込み対象ログファイルを並列実行用のリストを分ける
            if self.num_parallel == 1:
                # 並列プロセスが1の場合、分ける必要ない
                date_target_files.append(
                    {
                        "date": date,
                        "renamed_s3_files": renamed_s3_files
                    }
                )
            else:
                # プロセス毎に取り込み対象ログファイルを分ける
                # 例）プロセス数が４の場合、S3ログファイルパスのリストを4つ作成
                k, m = divmod(len(renamed_s3_files), self.num_parallel)
                files_parallels = [renamed_s3_files[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(self.num_parallel)]
                date_target_files.append(
                    {
                        "date": date,
                        "renamed_s3_files": list(files_parallels)
                    }
                )

        return date_target_files

    def validate_version(self, s3_object):
        # Bidcore開発環境
        if self.log_type == "bidcore_bid_request_app":
            return True

        # Bidcore本番環境
        if self.platform == "bidcore" and self.log_type == "bid_request_app":
            return True

        file_name = s3_object.key.split('/')[4]
        match = re.search(r'v{}'.format(self.version), file_name)
        return match

    def rename_s3_file_to_importing(self, original_path):
        try:
            path, file_name = original_path.rsplit('/', 1)
            new_file_name = '__' + file_name
            new_path = os.path.join(path, new_file_name)
            logger.info("Rename file s3://{}/{} → s3://{}/{}".format(
                self.bucket_name,
                path,
                self.bucket_name,
                new_path
            ))

            self.s3_resource.Object(
                self.bucket_name, new_path
            ).copy_from(CopySource=self.bucket_name+'/'+original_path)
            self.s3_resource.Object(self.bucket_name, original_path).delete()
            return new_path
        except Exception as e:
            msg = \
                'Failed to rename target S3 object. \
                [platform={}, log_type={}, file name={}, msg={}]'.format(
                    self.platform,
                    self.log_type,
                    original_path,
                    e
                )
            logger.error(msg)
            raise RuntimeError(msg)

    def get_num_parallel(self):
        """
        Get number of parallel processes
        """
        return self.num_parallel

    def get_version(self):
        """
        Get version
        """
        return self.version

    def get_columns(self):
        """
        Get metadata columns
        """
        return self.columns


class S3LogImporter(object):
    """
    Class for executing log import.
    """
    def __init__(self, platform, log_type, run_env, columns, version, date, parallel_num):
        with open('config.yml', 'r') as yml:
            self.config = yaml.load(yml, Loader=yaml.SafeLoader)[run_env]
        self.platform = platform
        self.log_type = log_type
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = self.config['bucket_name']
        self.s3_bucket = self.s3_resource.Bucket(self.bucket_name)
        self.columns = columns
        self.version = int(version)
        self.date = date
        self.parallel_num = parallel_num
        self.get_apikey()

        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)

    def log_info(self, msg):
        """
        Log info message to logger
        """
        logger.info("Process {}: {} [{}]".format(self.parallel_num, msg, self.date.strftime('%Y-%m-%d')))

    def log_error(self, msg):
        """
        Log error message to logger
        """
        logger.error("Process {}: {} [{}]".format(self.parallel_num, msg, self.date.strftime('%Y-%m-%d')))

    def log_warning(self, msg):
        """
        Log warning message to logger
        """
        logger.warning("Process {}: {} [{}]".format(self.parallel_num, msg, self.date.strftime('%Y-%m-%d')))

    def get_apikey(self):
        """
        Get apikey from parameter store
        """
        ssm_client = boto3.client('ssm')

        try:
            self.log_info('Getting TD API key from ParameterStore')
            response = ssm_client.get_parameters(
                Names=[self.config['td_apikey']],
                WithDecryption=True
            )
            self.td_apikey = response['Parameters'][0]['Value']
        except Exception:
            self.log_error('Can not get TD API key')
            raise RuntimeError('Can not get TD API key')

    def run(self, renamed_s3_filepaths, td_timeout, hour='00'):
        """
        Import logs using td import commands
        """
        if not renamed_s3_filepaths or len(renamed_s3_filepaths) == 0:
            self.log_info('target file is nothing. skip [{}]'.format(self.date.strftime('%Y-%m-%d')))
            return

        try:
            # エラー発生する場合、ロールバックできるフラグ
            can_rollback = True

            target_files = []
            for renamed_s3_filepath in renamed_s3_filepaths:
                # ローカルにダウンロード
                renamed_local_filepath = self._download_to_local(
                    renamed_s3_filepath
                )
                target_files.append(renamed_local_filepath)

                # ダウンロードしたファイルにtimeカラムを追加
                self._add_time_column_to_localfile(renamed_local_filepath)

            with td.Client(apikey=self.td_apikey) as cl:
                # Bulk importセッションのobjを作成
                obj = cl.create_bulk_import_session(
                    platform=self.platform,
                    log_type=self.log_type,
                    date=self.date,
                    hour=hour,
                    parallel_num=self.parallel_num,
                    logger=logger
                )
                bulk_import_obj = bulk_import.BulkImport(obj, logger, self.parallel_num)

                # Bulk importでアップロード
                for target_file in target_files:
                    bulk_import_obj.import_upload(target_file, self.columns)

                # Bulk import freeze
                bulk_import_obj.import_freeze()

                # Bulk import perform
                bulk_import_obj.import_perform(timeout=td_timeout)

                # Bulk import commit
                bulk_import_obj.import_commit()

                # Bulk commitした後、エラー発生してもロールバックされない
                can_rollback = False

                # 取り込み済みファイルをリネーム
                self.rename_s3_files_to_imported(
                    self.date.strftime('%Y-%m-%d'),
                    bulk_import_obj.list_parts()
                )

                # Bulk import delete session
                bulk_import_obj.delete_import_session()
        except Exception as e:
            # エラーが発生した場合、ロールバックを実行
            if can_rollback:
                self.rollback_s3_files_from_importing_to_not_import(renamed_s3_filepaths)

            raise

    def rename_s3_files_to_imported(self, str_date, parts_list):
        upload_parts = self.check_upload_parts(parts_list)
        imported_renamed_list = []

        for upload_part in upload_parts:
            new_file_name = upload_part[1:]
            original_path = '/'.join(
                ['data', self.platform, self.log_type, str_date, upload_part]
            )
            new_path = '/'.join(
                ['data', self.platform, self.log_type, str_date, new_file_name]
            )

            if new_path in imported_renamed_list:
                self.log_info(
                    'Log file was already renamed. new_path: {}'.format(
                        new_path
                    )
                )
                continue

            self.log_info('new_path:' + new_file_name)
            self.log_info('original_path:' + original_path)
            try:
                self.s3_resource.Object(
                    self.bucket_name, new_path
                ).copy_from(CopySource=self.bucket_name+'/'+original_path)
                self.s3_resource.Object(
                    self.bucket_name, original_path
                ).delete()

                imported_renamed_list.append(new_path)
            except Exception as e:
                msg = \
                    'Failed to rename imported S3 object. \
                    [platform={}, log_type={}, file name={}, msg={}]'.format(
                        self.platform,
                        self.log_type,
                        original_path,
                        e
                    )
                self.log_error(msg)
                raise RuntimeError(msg)

    def rollback_s3_files_from_importing_to_not_import(self, renamed_importing_paths):
        self.log_info("Log import error. Process rollback rename files from importing to not import")

        for importing_file_path in renamed_importing_paths:
            path, importing_file_name = importing_file_path.rsplit('/', 1)
            original_file_name = importing_file_name.lstrip("_")
            original_file_path = os.path.join(path, original_file_name)

            try:
                s3 = boto3.client("s3")
                s3.head_object(Bucket=self.bucket_name, Key=importing_file_path)
            except ClientError:
                self.log_warning(
                    "Importing file s3://{}/{} is not existed. Skip rollback this file.".format(
                        self.bucket_name,
                        importing_file_path
                    )
                )
                continue

            try:
                self.log_info("Rollback rename file s3://{}/{} → s3://{}/{}".format(
                    self.bucket_name,
                    importing_file_path,
                    self.bucket_name,
                    original_file_path
                ))

                self.s3_resource.Object(
                    self.bucket_name, original_file_path
                ).copy_from(CopySource=self.bucket_name+'/'+importing_file_path)
                self.s3_resource.Object(self.bucket_name, importing_file_path).delete()
            except Exception as e:
                msg = "Failed to rollback rename file. Skip this file. [file_path={}]".format(
                    importing_file_path,
                )
                self.log_error(msg)
                continue

    def check_upload_parts(self, parts_list):
        upload_parts = []
        self.log_info('check uploaded parts')
        for part in parts_list:
            words = part.strip().split('_')
            file_name_format = \
                self.config[self.platform][self.log_type]['file_name_format']
            file_name = eval(file_name_format)
            self.log_info(file_name)
            upload_parts.append(file_name)
        return upload_parts

    def _download_to_local(self, s3_path):
        """
        指定S3パスからローカルにダウンロードする
        """
        # ファイル名を取得
        file_name = s3_path.split('/')[-1]

        # ローカルファイルパスを設定
        download_file_path = os.path.join(self.data_dir, file_name)

        self.log_info(
            "Download file s3://{}/{} → {}".format(
                self.bucket_name, s3_path, download_file_path
            )
        )

        try:
            self.s3_bucket.download_file(s3_path, download_file_path)
        except Exception:
            logging.error("Error occured when download file.")
            raise

        return download_file_path

    def _add_time_column_to_localfile(self, local_file):
        """
        ダウンロードしたファイルにtimeカラムを追加する
        """
        self.log_info("Add time column to local file [{}]".format(local_file))
        df = pd.read_table(local_file,
            header=None,
            dtype=object,
            quoting=csv.QUOTE_NONE
        )
        time_stamp = pd.to_datetime(df[0], format='%Y-%m-%d %H:%M:%S')


        # Bidcore開発環境
        if self.log_type == "bidcore_bid_request_app":
            time_stamp += datetime.timedelta(hours=9)
            df[0] = time_stamp
            df[0] = df[0].astype(str)

        # Bidcore本番環境
        if self.platform == "bidcore" and self.log_type == "bid_request_app":
            time_stamp += datetime.timedelta(hours=9)
            df[0] = time_stamp
            df[0] = df[0].astype(str)

        # for correct time when convert from datetime to unix time using pandas
        time_stamp -= datetime.timedelta(hours=9)
        time_stamp = time_stamp.view(np.int64) // 10 ** 9

        df.insert(0, 'time_stamp', time_stamp)
        df_pa_table = pa.Table.from_pandas(df)

        with pa.CompressedOutputStream(local_file, 'gzip') as out:
            options = arcsv.WriteOptions(include_header=False, delimiter='\t')
            arcsv.write_csv(df_pa_table, out, options)
        self.log_info("complete add column file [{}]".format(local_file))


def daterange(_start, _end):
    for n in range((_end - _start).days + 1):
        yield _start + datetime.timedelta(n)


def log_import(parallel_num, td_timeout, date, start_time, renamed_s3_files, platform, log_type, run_env, columns, version):
    if not renamed_s3_files or len(renamed_s3_files) == 0:
        logger.info('Process {}: target file is nothing. skip [{}]'.format(parallel_num, date.strftime('%Y-%m-%d')))
        return

    obj = S3LogImporter(
        platform=platform,
        log_type=log_type,
        run_env=run_env,
        columns=columns,
        version=version,
        date=date,
        parallel_num=parallel_num
    )
    obj.run(
        renamed_s3_filepaths=renamed_s3_files,
        td_timeout=td_timeout,
        hour=start_time
    )


if __name__ == "__main__":
    stepfunction_name = os.environ.get('STEPFUNCTION_NAME')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    ecs_task_name = os.environ.get('ECS_TASK_NAME')
    platform = os.environ.get('LOG_PLATFORM')
    log_type = os.environ.get('LOG_TYPE')
    run_env = os.environ.get('RUN_ENV')
    version = os.environ.get('VERSION') or 0
    td_timeout = int(os.environ.get('TD_TIMEOUT'))
    log_files_limit = int(os.environ.get('LOG_FILES_LIMIT'))
    cur_log_stream = os.environ.get('LOG_STREAM')
    logger.info('version:' + str(version))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start-date",
        type=str,
        help="Strat date",
        required=True
    )
    parser.add_argument(
        "-e",
        "--end-date",
        type=str,
        help="End date",
        required=True
    )

    args = parser.parse_args()
    start_time = datetime.datetime.now().strftime('%H%M')
    if task_exists(run_env, cur_log_stream):
        logger.info(
            "Skip import because log_stream {} has run successfully before".format(
                cur_log_stream
            )
        )
        sys.exit()

    try:
        obj = LogImporterInitializer(
            platform,
            log_type,
            log_files_limit,
            run_env,
            version
        )
        date_target_files = obj.run(args.start_date, args.end_date)
        num_parallel = obj.get_num_parallel()
        columns = obj.get_columns()
        cur_version = obj.get_version()
    except Exception as e:
        message = "{} [stepfunction_name={}]".format(str(e), stepfunction_name)

        # SNSトピックに通知
        publish_err_message(
            task_name=ecs_task_name,
            topic_arn=sns_topic_arn,
            run_env=run_env,
            platform=platform,
            log_type=log_type,
            message=message
        )

    for date_target_file in date_target_files:
        try:
            date = date_target_file["date"]
            renamed_s3_files = date_target_file["renamed_s3_files"]

            if num_parallel == 1:
                # 並列プロセスが1の場合、順次実行
                log_import(
                    parallel_num=num_parallel,
                    td_timeout=td_timeout,
                    date=date,
                    start_time=start_time,
                    renamed_s3_files=renamed_s3_files,
                    platform=platform,
                    log_type=log_type,
                    run_env=run_env,
                    columns=columns,
                    version=cur_version
                )
            else:
                # プロセス毎に取り込みのパラメーターを作成
                params = []
                for index, renamed_s3_file in enumerate(renamed_s3_files):
                    parallel_num = index + 1
                    params.append(
                        (parallel_num, td_timeout, date, start_time, renamed_s3_file,
                         platform, log_type, run_env, columns, cur_version)
                    )

                # プロセスを作成して、取り込みスクリプトを実行
                with multiprocessing.Pool(num_parallel) as pool:
                    pool.starmap(log_import, params)
        except Exception as e:
            message = "{} [stepfunction_name={}]".format(str(e), stepfunction_name)

            # SNSトピックに通知
            publish_err_message(
                task_name=ecs_task_name,
                topic_arn=sns_topic_arn,
                run_env=run_env,
                platform=platform,
                log_type=log_type,
                message=message
            )
            continue