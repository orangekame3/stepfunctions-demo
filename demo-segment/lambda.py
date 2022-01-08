import boto3
import joblib
from boto3.session import Session
import os
import pandas as pd
import joblib
import tempfile
from pandas import json_normalize
import pickle

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
    recieve = "scatter/job.pkl"
    send = "gather/job.pkl"
    resp = s3_client.get_object(Bucket=bucket, Key=recieve)
    body = resp["Body"].read()
    # df = pd.DataFrame(pickle.loads(body))
    df = pickle.loads(body)
    df = json_normalize(
        df.to_dict("records"),
        "出勤状況",
        ["社員番号"],
    )
    s3_resource = session.resource(service_name="s3", endpoint_url=endpoint)
    with tempfile.TemporaryFile() as fp:
        pickle.dump(df, fp)
        fp.seek(0)
        s3_resource.Bucket(bucket).put_object(Key=send, Body=fp.read())
    return "done"
