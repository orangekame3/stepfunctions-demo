from __future__ import division
import pytest
import boto3
from scatter import ScatterHandler


@pytest.fixture
def scatter_handle():
    endpoint = "http://localhost:4566"
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )
    segments = {}
    segments["segment_definitions"] = []
    s3.put_object(
        Body="testdata/test_001.json",
        Bucket="aggregate",
        Key="data/test_001.json",
    )
    return ScatterHandler(event={}, context={}, s3=s3, segments=segments)


@pytest.mark.parametrize(
    "bucket,key,want",
    [
        (
            "aggregatebucket",
            "data/sample.json",
            [
                {
                    "会員番号": "000",
                    "名前": "松村 エリコ",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-06-28", "負荷レベル": 1},
                            "スクワット": {"実施日": "2021-06-28", "負荷レベル": 3},
                            "ベンチプレス": {"実施日": "2021-06-28", "負荷レベル": 2},
                        }
                    ],
                },
                {
                    "会員番号": "001",
                    "名前": "須賀 あいこ",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-07-24", "負荷レベル": 4},
                            "スクワット": {"実施日": "2021-07-24", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-07-24", "負荷レベル": 1},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-07-27", "負荷レベル": 3},
                            "スクワット": {"実施日": "2021-07-27", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-07-27", "負荷レベル": 2},
                        },
                    ],
                },
                {
                    "会員番号": "002",
                    "名前": "磯部 優",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-08-30", "負荷レベル": 5},
                            "スクワット": {"実施日": "2021-08-30", "負荷レベル": 1},
                            "ベンチプレス": {"実施日": "2021-08-30", "負荷レベル": 1},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-02", "負荷レベル": 5},
                            "スクワット": {"実施日": "2021-09-02", "負荷レベル": 1},
                            "ベンチプレス": {"実施日": "2021-09-02", "負荷レベル": 2},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-05", "負荷レベル": 2},
                            "スクワット": {"実施日": "2021-09-05", "負荷レベル": 3},
                            "ベンチプレス": {"実施日": "2021-09-05", "負荷レベル": 3},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-08", "負荷レベル": 2},
                            "スクワット": {"実施日": "2021-09-08", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-09-08", "負荷レベル": 2},
                        },
                    ],
                },
            ],
        ),
    ],
)
def test_get_s3_data(scatter_handle, bucket, key, want):
    assert scatter_handle.get_s3_data(bucket, key) == want


@pytest.mark.parametrize(
    "bucket,data,want",
    [
        (
            "aggregatebucket",
            [
                {
                    "会員番号": "000",
                    "名前": "松村 エリコ",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-06-28", "負荷レベル": 1},
                            "スクワット": {"実施日": "2021-06-28", "負荷レベル": 3},
                            "ベンチプレス": {"実施日": "2021-06-28", "負荷レベル": 2},
                        }
                    ],
                },
                {
                    "会員番号": "001",
                    "名前": "須賀 あいこ",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-07-24", "負荷レベル": 4},
                            "スクワット": {"実施日": "2021-07-24", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-07-24", "負荷レベル": 1},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-07-27", "負荷レベル": 3},
                            "スクワット": {"実施日": "2021-07-27", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-07-27", "負荷レベル": 2},
                        },
                    ],
                },
                {
                    "会員番号": "002",
                    "名前": "磯部 優",
                    "トレーニング履歴": [
                        {
                            "デッドリフト": {"実施日": "2021-08-30", "負荷レベル": 5},
                            "スクワット": {"実施日": "2021-08-30", "負荷レベル": 1},
                            "ベンチプレス": {"実施日": "2021-08-30", "負荷レベル": 1},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-02", "負荷レベル": 5},
                            "スクワット": {"実施日": "2021-09-02", "負荷レベル": 1},
                            "ベンチプレス": {"実施日": "2021-09-02", "負荷レベル": 2},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-05", "負荷レベル": 2},
                            "スクワット": {"実施日": "2021-09-05", "負荷レベル": 3},
                            "ベンチプレス": {"実施日": "2021-09-05", "負荷レベル": 3},
                        },
                        {
                            "デッドリフト": {"実施日": "2021-09-08", "負荷レベル": 2},
                            "スクワット": {"実施日": "2021-09-08", "負荷レベル": 4},
                            "ベンチプレス": {"実施日": "2021-09-08", "負荷レベル": 2},
                        },
                    ],
                },
            ],
            {"segment_definitions": ["scatter/job_000.pkl", "scatter/job_001.pkl"]},
        ),
    ],
)
def test_make_segment_df(scatter_handle, bucket, data, want):
    division_number = 1
    df = scatter_handle.make_df(data)
    dfs = [
        df.loc[i : i + division_number - 1, :]
        for i in range(0, len(df), division_number)
    ]
    assert scatter_handle.make_segment_df(scatter_handle.segments, bucket, dfs) == want


@pytest.mark.parametrize(
    "bucket,key,want",
    [
        (
            "aggregatebucket",
            "data/sample.json",
            {"segment_definitions": ["scatter/job_000.pkl", "scatter/job_001.pkl"]},
        ),
    ],
)
def test_main(scatter_handle, bucket, key, want):
    assert scatter_handle.main(bucket, key) == want
