import json
import os

import boto3


sns_client = boto3.client("sns")
FAILURES_TOPIC_ARN = os.environ["FAILURES_TOPIC_ARN"]


def lambda_handler(event, _context):
    error_payload = event.get("error", event)
    message = json.dumps(error_payload, default=str)

    sns_client.publish(
        TopicArn=FAILURES_TOPIC_ARN,
        Subject="Crypto pipeline failure",
        Message=message
    )

    return {
        "notification_sent": True,
        "message": message
    }
