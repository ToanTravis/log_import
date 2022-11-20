import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import yaml
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LogImportStepFunctionLauncher:
    def __init__(
        self,
        log_platform,
        log_type,
        run_env,
        start_date,
        end_date,
        version,
        log_files_limit
    ):
        self.s3 = boto3.client('s3')
        self.sns_client = boto3.client('sns')

        self.log_platform = log_platform
        self.log_type = log_type
        self.run_env = run_env
        self.config = None
        self.start_date = start_date
        self.end_date = end_date
        self.version = version
        self.log_files_limit = log_files_limit

    def get_config(self):
        with open('config.yaml', 'r') as fp:
            config_obj = yaml.load(fp, Loader=yaml.SafeLoader)

        if not config_obj.get(self.run_env, False):
            msg = f"Not have config for env {self.run_env}"
            logger.error(msg)
            raise ValueError(msg)

        self.config = config_obj[self.run_env]

    def _get_latest_metadata_version(self):
        metadata_path = '/'.join(['data', self.log_platform, 'metadata', ''])

        results = self.s3.list_objects_v2(
            Bucket=self.config['s3_bucket'],
            Prefix=metadata_path,
            Delimiter="/"
        )

        metadata_versions = []
        for o in results.get('CommonPrefixes'):
            version = o.get('Prefix').rstrip('/').split('/')[-1].lstrip('v')
            metadata_versions.append(int(version))

        if metadata_versions:
            return max(metadata_versions)

        return 0

    def validate_schema_file(self):

        s3_bucket = self.config['s3_bucket']
        if self.version == '0':
            target_version = self._get_latest_metadata_version()
        else:
            target_version = self.version

        schema_path = self.config['schema_path_prefix'].format(
            log_platform=self.log_platform,
            version=target_version,
            log_type=self.log_type
        )

        try:
            self.s3.get_object(Bucket=s3_bucket, Key=schema_path)
        except ClientError as e:
            self.log(
                f"Error when get schema file \
                    s3://{s3_bucket}/{schema_path}.\n{str(e)}"
            )
            if e.response['Error']['Code'] == "404":
                return

            raise

    def _build_stepfunction_params(self):
        task_cpu_size = self.config["machine_size"][self.log_platform][self.log_type]["cpu"]
        task_memory_size = self.config["machine_size"][self.log_platform][self.log_type]["memory"]
        router_memory_size = self.config["log_router_memory_size"]
        app_memory_size = int(task_memory_size) - router_memory_size
        td_timeout = str(self.config["td_timeout"])
        log_files_limit = str(self.config["log_files_limit"]) if self.log_files_limit == 0 else str(self.log_files_limit)
        log_time = datetime.now(timezone(timedelta(hours=+9), 'JST')).strftime('%Y-%m-%d-%H%M')

        return {
            "log_platform": self.log_platform,
            "log_type": self.log_type,
            "run_env": self.run_env,
            "version": self.version,
            "ecs_task_name": self.config['ecs_task_name'],
            "td_timeout": td_timeout,
            "log_files_limit": log_files_limit,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "log_stream": "{}_{}_{}".format(self.log_platform, self.log_type, log_time),
            "stepfunction_name": "{}_{}_{}".format(self.log_platform, self.log_type, log_time),
            "task_cpu_size": task_cpu_size,
            "task_memory_size": task_memory_size,
            "app_memory_size": app_memory_size,
            "router_memory_size": router_memory_size
        }

    def start_stepfunction(self):
        # Populate the required parameters to invoke step function
        params = self._build_stepfunction_params()
        state_machine_arn = os.environ["STATE_MACHINE_ARN"]

        try:
            client = boto3.client('stepfunctions')
            response = client.start_execution(
                name=params["stepfunction_name"],
                stateMachineArn=state_machine_arn,
                input=json.dumps(params)
            )
            print(response)
        except Exception as e:
            self.log(f"Error when start Step Function.\n{str(e)}")
            raise

    def log(self, msg):
        logger.error(msg)

        sns_topic_arn = os.environ['SNS_TOPIC_ARN']
        current_time = datetime.now(timezone.utc)

        sns_msg = {
            'message_type': 'error',
            'task_name': 'lambda-start-fargate',
            'timestamp': str(current_time),
            'run_env': self.run_env,
            'log_platform': self.log_platform,
            'log_type': self.log_type,
            'log_message': msg
        }

        self.sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(sns_msg)
        )

    def run(self):
        self.get_config()
        self.validate_schema_file()
        self.start_stepfunction()


def lambda_handler(event, context):
    environment = os.environ['ENV']
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST)
    yesterday = today - timedelta(days=1)
    default_start_date = yesterday.strftime("%Y-%m-%d")
    default_end_date = today.strftime("%Y-%m-%d")
    start_date = event.get('start_date', default_start_date)
    end_date = event.get('end_date', default_end_date)
    start_condition = datetime.strptime(start_date, "%Y-%m-%d")
    end_condition = datetime.strptime(end_date, "%Y-%m-%d")
    if (start_condition > end_condition):
        msg = "Target dates are invalid."
        logger.error(msg)
        raise Exception(msg)

    obj = LogImportStepFunctionLauncher(
        log_platform=event['platform'],
        log_type=event['type'],
        run_env=environment,
        start_date=start_date,
        end_date=end_date,
        version=event.get('version', '0'),
        log_files_limit=event.get('log_files_limit', 0)
    )
    obj.run()