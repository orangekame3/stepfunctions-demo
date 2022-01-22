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
    aggregate_bucket = "aggregatebucket"
    s3.create_bucket(
        Bucket=aggregate_bucket,
    )
    return ScatterHandler(
        event={},
        context={},
        s3=s3,
        segments=segments,
        aggregate_bucket=aggregate_bucket,
        recieve="",
        division_number=2,
        date_dir="",
    )


def s3_upload(s3, input, bucket, s3_path):
    with open(input, "rb") as data:
        s3.upload_fileobj(data, bucket, s3_path)


@pytest.mark.parametrize(
    "input,recieve,want",
    [
        (
            "tests/testdata/test_001.json",
            "data/2021/11/01/test_001.json",
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
def test_get_s3_data(scatter_handle, input, recieve, want):
    s3_upload(scatter_handle.s3, input, scatter_handle.aggregate_bucket, recieve)
    scatter_handle.recieve = recieve
    assert scatter_handle.get_s3_data() == want


@pytest.mark.parametrize(
    "json_dict,date_dir,want",
    [
        (
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
            "2021/11/01",
            {
                "segment_definitions": [
                    "scatter/2021/11/01/job_000.pkl",
                    "scatter/2021/11/01/job_001.pkl",
                ]
            },
        ),
    ],
)
def test_make_segment_df(scatter_handle, json_dict, date_dir, want):
    scatter_handle.division_number = 2
    scatter_handle.date_dir = date_dir
    df = scatter_handle.make_df(json_dict)
    dfs = [
        df.loc[i : i + scatter_handle.division_number - 1, :]
        for i in range(0, len(df), scatter_handle.division_number)
    ]
    assert scatter_handle.make_segment_df(dfs) == want


@pytest.mark.parametrize(
    "input,recieve,date_dir,want",
    [
        (
            "tests/testdata/test_001.json",
            "data/2021/11/01/test_001.json",
            "2021/11/01",
            {
                "segment_definitions": [
                    "scatter/2021/11/01/job_000.pkl",
                    "scatter/2021/11/01/job_001.pkl",
                ]
            },
        ),
    ],
)
def test_main(scatter_handle, input, recieve, date_dir, want):
    s3_upload(scatter_handle.s3, input, scatter_handle.aggregate_bucket, recieve)
    scatter_handle.recieve = recieve
    scatter_handle.date_dir = date_dir
    assert scatter_handle.main() == want
