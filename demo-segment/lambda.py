import logging
import os
import pickle
import tempfile

import boto3
from pandas import json_normalize

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

logger = logging.getLogger()

logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    bucket = "aggregatebucket"
    logger.info(f"event: {event}")
    recieve = event
    send = recieve.replace("scatter", "gather")
    resp = s3_client.get_object(Bucket=bucket, Key=recieve)
    body = resp["Body"].read()
    df = pickle.loads(body)
    df = json_normalize(
        df.to_dict("records"),
        "接種記録",
        ["社員番号"],
    )
    with tempfile.TemporaryFile() as fp:
        pickle.dump(df, fp)
        fp.seek(0)
        s3_client.put_object(
            Body=fp.read(),
            Bucket=bucket,
            Key=send,
        )
    return send
