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
    return ScatterHandler(event={}, context={}, s3=s3, segments=segments)


@pytest.mark.parametrize(
    "bucket,key,want",
    [
        (
            "aggregatebucket",
            "data/sample.json",
            [
                {
                    "社員番号": "0",
                    "接種記録": [
                        {
                            "モ○ルナ": {"接種日": "2021-12-27", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2022-01-11", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2022-01-26", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                    ],
                },
                {
                    "社員番号": "1",
                    "接種記録": [
                        {
                            "モ○ルナ": {"接種日": "2019-08-09", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-08-24", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-09-08", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-09-23", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
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
                    "社員番号": "0",
                    "接種記録": [
                        {
                            "モ○ルナ": {"接種日": "2021-12-27", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2022-01-11", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2022-01-26", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                    ],
                },
                {
                    "社員番号": "1",
                    "接種記録": [
                        {
                            "モ○ルナ": {"接種日": "2019-08-09", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-08-24", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-09-08", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
                        },
                        {
                            "モ○ルナ": {"接種日": "2019-09-23", "状態": "接種"},
                            "ファ○ザー": {"接種日": "xxxx-xx-xx", "状態": "未接種"},
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
