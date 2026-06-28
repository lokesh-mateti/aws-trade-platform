import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]


def handler(event, context):
    detail = event.get("detail", {})

    # Validate required fields
    required = ["event_id", "ticker", "price", "quantity", "side", "event_time"]
    missing = [f for f in required if f not in detail]
    if missing:
        log.error("Missing fields: %s", missing)
        return {"statusCode": 400, "body": f"Missing fields: {missing}"}

    # Build S3 key partitioned by date and ticker
    trade_date = detail.get("trade_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ticker     = detail["ticker"]
    key = f"trades/date={trade_date}/ticker={ticker}/{detail['event_id']}.json"

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(detail),
        ContentType="application/json",
    )

    log.info("Written to s3://%s/%s", RAW_BUCKET, key)
    return {"statusCode": 200, "body": key}
