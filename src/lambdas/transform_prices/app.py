import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3


s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

CURATED_BUCKET = os.environ["CURATED_BUCKET"]
LATEST_PRICES_TABLE = os.environ["LATEST_PRICES_TABLE"]
PIPELINE_RUNS_TABLE = os.environ["PIPELINE_RUNS_TABLE"]
DAILY_ALERT_THRESHOLD_PCT = Decimal(os.environ.get("DAILY_ALERT_THRESHOLD_PCT", "5"))


def _to_decimal(value):
    return Decimal(str(value))


def _serialize_decimal(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Unsupported type: {type(value)!r}")


def lambda_handler(event, _context):
    raw_bucket = event["raw_bucket"]
    raw_key = event["raw_key"]
    run_id = event["run_id"]

    raw_object = s3_client.get_object(Bucket=raw_bucket, Key=raw_key)
    raw_record = json.loads(raw_object["Body"].read().decode("utf-8"))

    latest_prices_table = dynamodb.Table(LATEST_PRICES_TABLE)
    pipeline_runs_table = dynamodb.Table(PIPELINE_RUNS_TABLE)

    processed_at = datetime.now(timezone.utc)
    records = []
    alerts = []

    for item in raw_record["data"]:
        symbol = item["symbol"].upper()
        asset_id = item["id"]
        current_price = _to_decimal(item["current_price"])
        previous_item = latest_prices_table.get_item(Key={"asset_symbol": symbol}).get("Item")
        previous_price = _to_decimal(previous_item["price_usd"]) if previous_item else None

        daily_change_abs = None
        daily_change_pct = None
        should_alert = False

        if previous_price not in (None, Decimal("0")):
            daily_change_abs = current_price - previous_price
            daily_change_pct = (daily_change_abs / previous_price) * Decimal("100")
            should_alert = abs(daily_change_pct) >= DAILY_ALERT_THRESHOLD_PCT

        price_timestamp = item.get("last_updated", processed_at.isoformat())
        record = {
            "asset_id": asset_id,
            "asset_symbol": symbol,
            "asset_name": item["name"],
            "price_usd": current_price,
            "as_of_timestamp": price_timestamp,
            "as_of_date": price_timestamp[:10],
            "daily_change_abs": daily_change_abs,
            "daily_change_pct": daily_change_pct,
            "source": raw_record["source"],
            "currency": raw_record["currency"],
            "pipeline_run_id": run_id,
            "processed_at": processed_at.isoformat()
        }
        records.append(record)

        latest_prices_table.put_item(Item={
            "asset_symbol": symbol,
            "asset_id": asset_id,
            "asset_name": item["name"],
            "price_usd": current_price,
            "last_updated": price_timestamp,
            "pipeline_run_id": run_id,
            "updated_at": processed_at.isoformat()
        })

        if should_alert:
            alerts.append({
                "asset_symbol": symbol,
                "asset_name": item["name"],
                "price_usd": float(current_price),
                "daily_change_pct": float(daily_change_pct),
                "daily_change_abs": float(daily_change_abs)
            })

    curated_key = (
        f"year={processed_at:%Y}/month={processed_at:%m}/day={processed_at:%d}/"
        f"run_id={run_id}/prices.ndjson"
    )
    curated_body = "\n".join(json.dumps(record, default=_serialize_decimal) for record in records)

    s3_client.put_object(
        Bucket=CURATED_BUCKET,
        Key=curated_key,
        Body=curated_body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    pipeline_runs_table.update_item(
        Key={"run_id": run_id},
        UpdateExpression="SET #status = :status, curated_s3_key = :curated_s3_key, processed_count = :processed_count, processed_at = :processed_at",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "PROCESSED",
            ":curated_s3_key": curated_key,
            ":processed_count": len(records),
            ":processed_at": processed_at.isoformat()
        }
    )

    return {
        "run_id": run_id,
        "curated_bucket": CURATED_BUCKET,
        "curated_key": curated_key,
        "processed_count": len(records),
        "alerts": alerts,
        "threshold_pct": float(DAILY_ALERT_THRESHOLD_PCT)
    }
