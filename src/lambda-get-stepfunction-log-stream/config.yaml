import os
import json
import logging
import time
from datetime import datetime, timedelta

import boto3
import yaml

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StepFunctionLogStream:
    def __init__(self, run_env):
        self.log_client = boto3.client("logs")
        self.run_env = run_env

        with open('config.yaml', 'r') as fp:
            self.config = yaml.load(fp, Loader=yaml.SafeLoader)[self.run_env]

    def get_log_stream(self, stepfunction_name):
        log_group = self.config["log_groups"]
        wait_time = datetime.now() + timedelta(seconds=self.config["wait_time"])

        try:
            while datetime.now() < wait_time:
                log_streams = self.log_client.describe_log_streams(
                    logGroupName=log_group,
                    orderBy="LastEventTime"
                )

                for log_stream in log_streams["logStreams"]:
                    log_stream_name = log_stream["logStreamName"]
                    log_events = self.log_client.get_log_events(
                        logGroupName=log_group,
                        logStreamName=log_stream_name
                    )

                    for log_event in log_events["events"]:
                        try:
                            message = json.loads(log_event["message"])
                        except:
                            continue

                        if message["type"] != "ExecutionStarted":
                            continue

                        data = json.loads(message["details"]["input"])
                        if data["stepfunction_name"] == stepfunction_name:
                            logger.info("Step Function name = {}. Log stream name = {}".format(
                                stepfunction_name,
                                log_stream_name
                            ))
                            return log_stream_name
                time.sleep(1)

            return self.config["default_log_stream"]
        except:
            logger.error("Can not get stepfunctions log stream. Skip...")
            return self.config["default_log_stream"]


def lambda_handler(event, context):
    environment = os.environ['ENV']
    stepfunction_name = event.get("stepfunction_name")

    obj = StepFunctionLogStream(
        run_env=environment
    )

    return obj.get_log_stream(stepfunction_name)