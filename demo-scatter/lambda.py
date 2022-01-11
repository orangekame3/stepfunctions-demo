import json
import os
import pickle
import tempfile

import boto3
import pandas as pd

if os.getenv("LOCALSTACK_HOSTNAME") is None:
    endpoint = "http://localhost:4566"
    s3_client = boto3.client("s3", "ap-northeast-1")
else:
    endpoint = f"http://{os.environ['LOCALSTACK_HOSTNAME']}:4566"
    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )


def lambda_handler(event, context):

    bucket = "aggregatebucket"
    recieve = "data/sample.json"
    send = "scatter/"
    resp = s3_client.get_object(Bucket=bucket, Key=recieve)
    body = resp["Body"].read().decode("utf-8")
    sample = json.loads(body)
    sample = json.dumps(sample)
    df = pd.read_json(sample)

    k = 10
    dfs = [df.loc[i : i + k - 1, :] for i in range(0, len(df), k)]
    task = {}
    task["task_definitions"] = []
    for i, df_i in enumerate(dfs):
        with tempfile.TemporaryFile() as fp:
            pickle.dump(df_i, fp)
            fp.seek(0)
            fsend = send + str(i).zfill(3) + "_job.pkl"
            s3_client.put_object(
                Body=fp.read(),
                Bucket=bucket,
                Key=fsend,
            )
            task["task_definitions"].append(fsend)
    return task
