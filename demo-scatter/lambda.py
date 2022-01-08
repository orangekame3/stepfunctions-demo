import boto3
import pandas
import joblib
from boto3.session import Session
import os
from datetime import datetime
import json
import pandas as pd
import joblib
import tempfile

session = Session(
    aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1"
)
if os.getenv("LOCALSTACK_HOSTNAME") is None:
    endpoint = "http://localhost:4566"
else:
    endpoint = f"http://{os.environ['LOCALSTACK_HOSTNAME']}:4566"


s3_client = session.client(service_name="s3", endpoint_url=endpoint)


def lambda_handler(event, context):

    bucket = "test-bucket"
    key = "test-data/sample.json"
    send = "scatter/job.pkl"
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")
    sample = json.loads(body)
    # df = pd.read_json(sample)
    df = pd.DataFrame(sample)
    s3_resource = session.resource(service_name="s3", endpoint_url=endpoint)
    with tempfile.TemporaryFile() as fp:
        joblib.dump(df, fp)
        fp.seek(0)
        s3_resource.Bucket(bucket).put_object(Key=send, Body=fp.read())
    return sample
