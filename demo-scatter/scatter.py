import json
import tempfile
import logging
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)

mybucket = "aggregatebucket"
recieve = "data/sample.json"
send = "scatter/job_"
segment_task_key = "segment_definitions"
division_number = 1


class ScatterHandler(object):
    def __init__(self, event, context, s3, segments):
        self.event = event
        self.context = context
        self.s3 = s3
        self.segments = segments

    def main(self, bucket=mybucket, key=recieve) -> dict:
        try:
            data = self.get_s3_data(bucket, key)
            df = self.make_df(data)
            dfs = [
                df.loc[i : i + division_number - 1, :]
                for i in range(0, len(df), division_number)
            ]
            segments = self.make_segment_df(self.segments, bucket, dfs)
            return segments

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self, bucket, key) -> str:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
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
