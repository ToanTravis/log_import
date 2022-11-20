import json
import logging

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client('ssm')


def get_slack_webhook(run_env):
    parameter_name = "DEVELOPMENT-SLACK-WEBHOOK"
    if run_env == "production":
        parameter_name = "PRODUCTION-SLACK-WEBHOOK"

    parameters = ssm.get_parameters(
        Names=[parameter_name],
        WithDecryption=True
    )

    return parameters['Parameters'][0]['Value']


def build_slack_message_block(sns_data):
    """
    Build slack block message

    :param sns_data:
    :return:

    building layout https://api.slack.com/messaging/composing/layouts
    """
    task_name = sns_data.get('task_name', 'NotProvided')
    headline = f'a1-log-import/{task_name}'
    message_type = sns_data['message_type'].upper()

    slack_message_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": headline
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*severity:*\n{message_type}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*timestamp:*\n{sns_data['timestamp']}"
                }
            ]
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*task_name:*\n{task_name}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*environment:*\n{sns_data['run_env']}"
                }
            ]
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*log_platform:*\n{sns_data['log_platform']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*log_type:*\n{sns_data['log_type']}"
                }
            ]
        }
    ]

    if sns_data.get("cloudwatch_log_url"):
        slack_message_block.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Cloudwatch Logs:*\n<{sns_data['cloudwatch_log_url']}|URL>"
                }
            ]
        })

    slack_message_block.extend([
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": sns_data['log_message']
            }
        }
    ])

    return slack_message_block


def lambda_handler(event, context):
    sns_message = event['Records'][0]['Sns']['Message']
    sns_message = json.loads(sns_message)

    msg_color = {
        'warning': '#f2c744',
        'error': '#e70707',
        'info': '#d0d0d0'
    }

    color = msg_color.get(sns_message['message_type'].lower(), msg_color['info'])
    slack_msg = {
        'attachments': [
            {
                "color": f"{color}",
                "blocks": build_slack_message_block(sns_message)
            }
        ]
    }

    slack_webhook = get_slack_webhook(sns_message['run_env'])

    try:
        r = requests.post(slack_webhook, json=slack_msg)

        # Handling error: https://api.slack.com/messaging/webhooks#handling_errors
        r.raise_for_status()
    except Exception as e:
        logger.error(e)
        raise e