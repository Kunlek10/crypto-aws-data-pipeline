import json
import os

import boto3


sns_client = boto3.client("sns")
ALERTS_TOPIC_ARN = os.environ["ALERTS_TOPIC_ARN"]


def lambda_handler(event, _context):
    alerts = event.get("alerts", [])
    if not alerts:
        return {
            "run_id": event["run_id"],
            "notification_sent": False,
            "message": "No assets crossed the daily alert threshold."
        }

    lines = [
        f"{alert['asset_name']} ({alert['asset_symbol']}): "
        f"price=${alert['price_usd']:.2f}, "
        f"daily_change_pct={alert['daily_change_pct']:.2f}%, "
        f"daily_change_abs={alert['daily_change_abs']:.2f}"
        for alert in alerts
    ]
    body = "\n".join(lines)

    sns_client.publish(
        TopicArn=ALERTS_TOPIC_ARN,
        Subject=f"Crypto price alert for run {event['run_id']}",
        Message=body
    )

    return {
        "run_id": event["run_id"],
        "notification_sent": True,
        "alert_count": len(alerts),
        "alerts": alerts,
        "message": json.dumps(lines)
    }
