import json
import tempfile
import logging
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)

send = "scatter/job_"
segment_task_key = "segment_definitions"


class ScatterHandler(object):
    def __init__(self, event, context, s3, segments, bucket, recieve, division_number):
        self.event = event
        self.context = context
        self.s3 = s3
        self.segments = segments
        self.bucket = bucket
        self.recieve = recieve
        self.division_number = division_number

    def main(self) -> dict:
        try:
            data = self.get_s3_data()
            df = self.make_df(data)
            dfs = [
                df.loc[i : i + self.division_number - 1, :]
                for i in range(0, len(df), self.division_number)
            ]
            segments = self.make_segment_df(self.segments, self.bucket, dfs)
            return segments

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self) -> str:
        resp = self.s3.get_object(Bucket=self.bucket, Key=self.recieve)
        body = resp["Body"].read().decode("utf-8")
        json_dict = json.loads(body)
        return json_dict

    def make_df(self, data: str) -> pd.DataFrame:
        return pd.DataFrame.from_dict(data)

    def make_segment_df(self, segments: dict, bucket: str, dfs: list) -> dict:
        for i, df_i in enumerate(dfs):
            with tempfile.TemporaryFile() as fp:
                df_i.to_pickle(fp)
                fp.seek(0)
                fsend = send + str(i).zfill(3) + ".pkl"
                self.s3.put_object(
                    Body=fp.read(),
                    Bucket=bucket,
                    Key=fsend,
                )
                segments[segment_task_key].append(fsend)
        return segments
