import json
import tempfile
import logging
import pandas as pd
from typing import List, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ScatterHandler(object):
    def __init__(
        self,
        event,
        context,
        s3,
    ):
        self.event = event
        self.context = context
        self.s3 = s3

    def main(self) -> dict:
        try:
            bucket = "test-bucket"
            data_path = self.event["input_obj"]
            division_number = 50
            segments: Dict = {}
            segments["segment_definitions"] = []
            data = self.get_s3_data(bucket, data_path)
            df = self.make_df(data)
            dfs = [
                df.loc[i : i + division_number - 1, :]
                for i in range(0, len(df), division_number)
            ]
            segments = self.make_segment_df(segments, bucket, dfs)
            return segments

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self, bucket: str, key: str) -> List[dict]:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
        json_dict: List[dict] = json.loads(body)
        return json_dict

    def make_df(self, data: list) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(data)
        return df

    def make_segment_df(self, segments: dict, bucket: str, dfs: list) -> dict:
        for i, df_i in enumerate(dfs):
            with tempfile.TemporaryFile() as fp:
                df_i.to_pickle(fp)
                fp.seek(0)
                fsend = "scatter/job_" + str(i).zfill(3) + ".pkl"
                self.s3.put_object(
                    Body=fp.read(),
                    Bucket=bucket,
                    Key=fsend,
                )
                segments["segment_definitions"].append(fsend)
        return segments
