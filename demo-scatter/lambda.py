import os
import boto3
from scatter import ScatterHandler

if os.getenv("LOCALSTACK_HOSTNAME") is None:
    s3 = boto3.client("s3", "ap-northeast-1")
else:
    endpoint = f"http://{os.environ['LOCALSTACK_HOSTNAME']}:4566"
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )

segments = {}
segments["segment_definitions"] = []


def lambda_handler(event, context) -> dict:
    handler = ScatterHandler(event, context, s3, segments)
    return handler.main()
