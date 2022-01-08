import boto3
from boto3.session import Session
import os
import pandas as pd
import tempfile
import pickle

session = Session(
    aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1"
)
if os.getenv("LOCALSTACK_HOSTNAME") is None:
    endpoint = "http://localhost:4566"
    s3_client = boto3.client("s3", "ap-northeast-1")
else:
    endpoint = f"http://{os.environ['LOCALSTACK_HOSTNAME']}:4566"
    s3_client = session.client(service_name="s3", endpoint_url=endpoint)
    s3_resource = session.resource(service_name="s3", endpoint_url=endpoint)


def lambda_handler(event, context):
    bucket = "aggregatebucket"
    frames = []
    task_results = event
    send = "summary.xlsx"
    for pkl in task_results["task_results"]:
        resp = s3_client.get_object(Bucket=bucket, Key=pkl)
        body = resp["Body"].read()
        df = pickle.loads(body)
        frames.append(df)
    result = pd.concat(frames, axis=0)
    with tempfile.TemporaryFile() as fp:
        writer = pd.ExcelWriter(fp, engine="xlsxwriter")
        result.to_excel(writer, sheet_name="Sheet1", index=False)
        worksheet = writer.sheets["Sheet1"]
        writer.save()
        fp.seek(0)
        s3_client.put_object(
            Body=fp.read(),
            Bucket=bucket,
            Key=send,
        )
    return event
