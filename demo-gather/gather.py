import tempfile
import logging
import pandas as pd
import pickle
from typing import List

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GatherHandler(object):
    def __init__(self, event, context, s3):
        self.event = event
        self.context = context
        self.s3 = s3

    def main(self) -> str:
        try:
            bucket = "test-bucket"
            segments = self.event["segment_results"]
            send = "test.xlsx"
            data_frames: List[pd.DataFrame] = []
            for pkl in segments:
                df = self.get_s3_df(bucket, pkl)
                data_frames.append(df)
            df_gather = pd.concat(data_frames)
            return self.send_excel(df_gather, bucket, send)

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_df(self, bucket, key) -> pd.DataFrame:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        df = pickle.loads(body)
        return df

    def send_excel(self, df: pd.DataFrame, bucket: str, send: str) -> str:
        with tempfile.TemporaryFile() as fp:
            writer = pd.ExcelWriter(fp, engine="xlsxwriter")
            df.to_excel(writer, sheet_name="Sheet1", index=False)
            writer.save()
            fp.seek(0)
            self.s3.put_object(
                Body=fp.read(),
                Bucket=bucket,
                Key=send,
            )
        return send
