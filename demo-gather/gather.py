import json
import tempfile
import logging
import pandas as pd
import pickle

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mybucket = "aggregatebucket"
init_segments = {}
init_segments["segment_results"] = ["gather/job_000.pkl"]


class GatherHandler(object):
    def __init__(self, event, context, s3, data_frames):
        self.event = event
        self.context = context
        self.s3 = s3
        self.data_frames = data_frames

    def main(self, bucket=mybucket) -> dict:
        try:

            if self.event == {}:
                segments = init_segments["segment_results"]
            else:
                segments = self.event["segment_results"]

            send = "summary.xlsx"
            for pkl in segments:
                df = self.get_s3_df(bucket, pkl)
                self.data_frames.append(df)
            df_gather = pd.concat(self.data_frames)
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
            worksheet = writer.sheets["Sheet1"]
            writer.save()
            fp.seek(0)
            self.s3.put_object(
                Body=fp.read(),
                Bucket=bucket,
                Key=send,
            )
        return send
