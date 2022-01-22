import json
import tempfile
import logging
import pandas as pd
from pandas import json_normalize
import pickle

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mybucket = "aggregatebucket"


class SegmentHandler(object):
    def __init__(self, event, context, s3):
        self.event = event
        self.context = context
        self.s3 = s3

    def main(self, bucket=mybucket) -> dict:
        try:

            if self.event == {}:
                recieve = "scatter/job_000.pkl"
            else:
                recieve = self.event
            send = recieve.replace("scatter", "gather")
            df = self.get_s3_data(bucket, recieve)
            # df = self.make_df(data)
            df = self.normalize(df)
            return self.send_segment_df(df, bucket, send)

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self, bucket, key) -> pd.DataFrame:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        df = pickle.loads(body)
        return df

    def normalize(self, df) -> pd.DataFrame:
        return json_normalize(
            df.to_dict("records"),
            "接種記録",
            ["社員番号"],
        )

    def make_df(self, data: str) -> pd.DataFrame:
        return pd.DataFrame.from_dict(data)

    def send_segment_df(self, df: pd.DataFrame, bucket: str, send: str) -> str:
        with tempfile.TemporaryFile() as fp:
            df.to_pickle(fp)
            fp.seek(0)
            self.s3.put_object(
                Body=fp.read(),
                Bucket=bucket,
                Key=send,
            )
        return send
