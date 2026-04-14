import json
import os
from datetime import datetime, timezone
from urllib import parse, request

import boto3


s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

RAW_BUCKET = os.environ["RAW_BUCKET"]
PIPELINE_RUNS = os.environ["PIPELINE_RUNS"]
ASSETS = [asset.strip() for asset in os.environ.get("ASSETS", "bitcoin,ethereum").split(",") if asset.strip()]
DEFAULT_CURRENCY = os.environ.get("DEFAULT_CURRENCY", "usd")


def lambda_handler(event, _context):
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")

    query = parse.urlencode({
        "vs_currency": DEFAULT_CURRENCY,
        "ids": ",".join(ASSETS)
    })
    endpoint = f"https://api.coingecko.com/api/v3/coins/markets?{query}"

    with request.urlopen(endpoint, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    key = (
        f"year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"run_id={run_id}/prices.json"
    )

    raw_record = {
        "run_id": run_id,
        "ingested_at": now.isoformat(),
        "source": "coingecko",
        "currency": DEFAULT_CURRENCY,
        "assets": ASSETS,
        "data": payload
    }

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(raw_record).encode("utf-8"),
        ContentType="application/json"
    )

    runs_table = dynamodb.Table(PIPELINE_RUNS)
    runs_table.put_item(Item={
        "run_id": run_id,
        "status": "INGESTED",
        "raw_s3_key": key,
        "raw_bucket": RAW_BUCKET,
        "record_count": len(payload),
        "trigger_source": event.get("triggered_by", "manual"),
        "created_at": now.isoformat()
    })

    return {
        "run_id": run_id,
        "raw_bucket": RAW_BUCKET,
        "raw_key": key,
        "record_count": len(payload),
        "currency": DEFAULT_CURRENCY,
        "assets": ASSETS
    }
