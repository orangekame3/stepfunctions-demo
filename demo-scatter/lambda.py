import os
import boto3
from scatter import ScatterHandler
import datetime
import os
import glob

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
now = datetime.datetime.now()
recieve_date = "data/" + now.strftime("%Y/%m/%d")
bucket = "aggregatebucket"
recieve = s3.list_objects_v2(Bucket=bucket, Prefix=recieve_date)["Contents"][0]["Key"]
division_number = 10


def lambda_handler(event, context) -> dict:
    handler = ScatterHandler(
        event, context, s3, segments, bucket, recieve, division_number
    )
    return handler.main()
