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
    recieve = "test-data/sample.json"
    send = "scatter/"
    resp = s3_client.get_object(Bucket=bucket, Key=recieve)
    body = resp["Body"].read().decode("utf-8")
    sample = json.loads(body)
    sample = json.dumps(sample)
    # df = pd.read_json(sample)
    # df = pd.DataFrame(sample)
    df = pd.read_json(sample)
    s3_resource = session.resource(service_name="s3", endpoint_url=endpoint)
    k = 100
    dfs = [df.loc[i : i + k - 1, :] for i in range(0, len(df), k)]
    task = {}
    task["task_definitions"] = []
    for i, df_i in enumerate(dfs):
        with tempfile.TemporaryFile() as fp:
            pickle.dump(df_i, fp)
            fp.seek(0)
            fsend = send + str(i).zfill(3) + "_job.pkl"
            s3_resource.Bucket(bucket).put_object(Key=fsend, Body=fp.read())
            task["task_definitions"].append(fsend)
    return json.dumps(task)
