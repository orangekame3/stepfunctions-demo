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
date_dir = now.strftime("%Y/%m/%d")
data_path = "data/" + date_dir
aggregate_bucket = "aggregatebucket"
recieve = s3.list_objects_v2(Bucket=aggregate_bucket, Prefix=data_path)["Contents"][0][
    "Key"
]
division_number = 10
segment_task_key = "segment_definitions"


def lambda_handler(event, context) -> dict:
    handler = ScatterHandler(
        event,
        context,
        s3,
        segments,
        aggregate_bucket,
        recieve,
        division_number,
        date_dir,
    )
    return handler.main()
