from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
import boto3
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = APIGatewayRestResolver()
tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

s3_client = boto3.client('s3')
bucket_name = os.environ.get('S3_BUCKET_NAME')

def get_s3_key():
    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    date_dir = now.strftime('%Y/%m/%d')
    timestamp = now.strftime('%Y%m%d-%H%M%S-%f')[:-3]
    return f"{date_dir}/{timestamp}.json"

@app.post("/webhook")
@tracer.capture_method
def webhook():
    body = app.current_event.json_body
    logger.info("Webhook received", extra={"body": body})

    if not bucket_name:
        logger.error("S3 bucket not configured")
        return {}
    
    try:
        key = get_s3_key()
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(body, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        logger.info("Webhook data saved successfully", extra={
            "s3_bucket": bucket_name,
            "s3_key": key,
        })
        
        return {}
        
    except Exception as e:
        logger.error("Failed to save webhook data to S3", extra={
            "error": str(e),
            "s3_bucket": bucket_name,
            "s3_key": key,
        })
        return {}

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
