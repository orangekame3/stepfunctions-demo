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
            df = self.normalize(df)
            df = self.df_process(df)
            return self.send_segment_df(df, bucket, send)

        except Exception as e:
            logger.exception(e)
            raise e

    def get_s3_data(self, bucket, key) -> pd.DataFrame:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        df = pickle.loads(body)
        return df

    def df_calc_stress(self, row):
        return row["デッドリフト.負荷レベル"] + row["スクワット.負荷レベル"] + row["ベンチプレス.負荷レベル"]

    # def df_determine_date(self, df):
    #     return df.drop(["スクワット.実施日", "ベンチプレス.実施日"], axis=1)

    def df_rename(self, df):
        return df.rename(
            columns={
                "デッドリフト.実施日": "実施日",
                "デッドリフト.負荷レベル": "デッドリフト負荷レベル",
                "スクワット.負荷レベル": "スクワット負荷レベル",
                "ベンチプレス.負荷レベル": "ベンチプレス負荷レベル",
            }
        )

    def df_drop(self, df):
        return df.drop(columns=["スクワット.実施日", "ベンチプレス.実施日"], axis=1)

    def df_reindex(self, df):
        return df.reindex(
            columns=[
                "会員番号",
                "名前",
                "実施日",
                "デッドリフト負荷レベル",
                "スクワット負荷レベル",
                "ベンチプレス負荷レベル",
                "総負荷レベル",
            ]
        )

    def df_sort(self, df):
        return df.sort_values(by="実施日")

    def df_process(self, df) -> pd.DataFrame:
        df["総負荷レベル"] = df.apply(self.df_calc_stress, axis=1)
        df = self.df_rename(df)
        df = self.df_drop(df)
        df = self.df_reindex(df)
        df = self.df_sort(df)
        return df

    def normalize(self, df) -> pd.DataFrame:
        return json_normalize(
            df.to_dict("records"),
            "トレーニング履歴",
            ["会員番号", "名前"],
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
