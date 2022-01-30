import tempfile
import logging
import pickle
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SegmentHandler(object):
    def __init__(self, event, context, s3):
        self.event = event
        self.context = context
        self.s3 = s3

    def main(
        self,
    ) -> str:
        try:
            bucket = "test-bucket"
            recieve = self.event
            send = recieve.replace("scatter", "gather")
            df = self.get_s3_data(bucket, recieve)
            df = self.process(df)
            return self.send_segment_df(df, bucket, send)

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self, bucket, key) -> pd.DataFrame:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        df = pickle.loads(body)
        return df

    def calc(self, row):
        if row["会員ランク"] > 3:
            return row["ポイント"] * 1.25
        else:
            return row["ポイント"]

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        data["ボーナスポイント"] = data.apply(self.calc, axis=1)
        return data

    def make_df(self, data: list) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(data)
        return df

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
