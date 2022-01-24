import pytest
import boto3
from segment import SegmentHandler
import json
import pandas as pd


@pytest.fixture
def segment_handle():
    endpoint = "http://localhost:4566"
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )
    aggregate_bucket = "aggregatebucket"
    s3.create_bucket(
        Bucket=aggregate_bucket,
    )
    return SegmentHandler(
        event={},
        context={},
        s3=s3,
        aggregate_bucket=aggregate_bucket,
    )


# def s3_upload(s3, input, bucket, s3_path):
#     with open(input, "rb") as data:
#         s3.upload_fileobj(data, bucket, s3_path)


# @pytest.mark.parametrize(
#     "input,recieve,want",
#     [
#         (
#             "tests/testdata/test_001.json",
#             "data/2021/11/01/test_001.json",
#             [
#                 {
#                     "会員番号": "000",
#                     "名前": "松村 エリコ",
#                     "トレーニング履歴": [
#                         {
#                             "デッドリフト": {"実施日": "2021-06-28", "負荷レベル": 1},
#                             "スクワット": {"実施日": "2021-06-28", "負荷レベル": 3},
#                             "ベンチプレス": {"実施日": "2021-06-28", "負荷レベル": 2},
#                         }
#                     ],
#                 },
#                 {
#                     "会員番号": "001",
#                     "名前": "須賀 あいこ",
#                     "トレーニング履歴": [
#                         {
#                             "デッドリフト": {"実施日": "2021-07-24", "負荷レベル": 4},
#                             "スクワット": {"実施日": "2021-07-24", "負荷レベル": 4},
#                             "ベンチプレス": {"実施日": "2021-07-24", "負荷レベル": 1},
#                         },
#                         {
#                             "デッドリフト": {"実施日": "2021-07-27", "負荷レベル": 3},
#                             "スクワット": {"実施日": "2021-07-27", "負荷レベル": 4},
#                             "ベンチプレス": {"実施日": "2021-07-27", "負荷レベル": 2},
#                         },
#                     ],
#                 },
#                 {
#                     "会員番号": "002",
#                     "名前": "磯部 優",
#                     "トレーニング履歴": [
#                         {
#                             "デッドリフト": {"実施日": "2021-08-30", "負荷レベル": 5},
#                             "スクワット": {"実施日": "2021-08-30", "負荷レベル": 1},
#                             "ベンチプレス": {"実施日": "2021-08-30", "負荷レベル": 1},
#                         },
#                         {
#                             "デッドリフト": {"実施日": "2021-09-02", "負荷レベル": 5},
#                             "スクワット": {"実施日": "2021-09-02", "負荷レベル": 1},
#                             "ベンチプレス": {"実施日": "2021-09-02", "負荷レベル": 2},
#                         },
#                         {
#                             "デッドリフト": {"実施日": "2021-09-05", "負荷レベル": 2},
#                             "スクワット": {"実施日": "2021-09-05", "負荷レベル": 3},
#                             "ベンチプレス": {"実施日": "2021-09-05", "負荷レベル": 3},
#                         },
#                         {
#                             "デッドリフト": {"実施日": "2021-09-08", "負荷レベル": 2},
#                             "スクワット": {"実施日": "2021-09-08", "負荷レベル": 4},
#                             "ベンチプレス": {"実施日": "2021-09-08", "負荷レベル": 2},
#                         },
#                     ],
#                 },
#             ],
#         ),
#     ],
# )
# def test_get_s3_data(segment_handle, input, recieve, want):
#     s3_upload(segment_handle.s3, input, segment_handle.aggregate_bucket, recieve)
#     segment_handle.recieve = recieve
#     assert segment_handle.get_s3_data() == want


@pytest.mark.parametrize(
    "input_dict,want_dict",
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
            ],
            [
                {
                    "会員番号": "000",
                    "名前": "松村 エリコ",
                    "デッドリフト.実施日": "2021-06-28",
                    "スクワット.実施日": "2021-06-28",
                    "ベンチプレス.実施日": "2021-06-28",
                    "デッドリフト.負荷レベル": 1,
                    "スクワット.負荷レベル": 3,
                    "ベンチプレス.負荷レベル": 2,
                },
                {
                    "会員番号": "001",
                    "名前": "須賀 あいこ",
                    "デッドリフト.実施日": "2021-07-24",
                    "スクワット.実施日": "2021-07-24",
                    "ベンチプレス.実施日": "2021-07-24",
                    "デッドリフト.負荷レベル": 4,
                    "スクワット.負荷レベル": 4,
                    "ベンチプレス.負荷レベル": 1,
                },
                {
                    "会員番号": "001",
                    "名前": "須賀 あいこ",
                    "デッドリフト.実施日": "2021-07-27",
                    "スクワット.実施日": "2021-07-27",
                    "ベンチプレス.実施日": "2021-07-27",
                    "デッドリフト.負荷レベル": 3,
                    "スクワット.負荷レベル": 4,
                    "ベンチプレス.負荷レベル": 2,
                },
            ],
        ),
    ],
)
def test_df_normalize(segment_handle, input_dict, want_dict):
    json_dict = pd.DataFrame.from_dict(input_dict)
    got = segment_handle.df_normalize(json_dict).sort_index(axis=1, ascending=False)
    want = pd.DataFrame(want_dict).sort_index(axis=1, ascending=False)
    pd.testing.assert_frame_equal(got, want)


# @pytest.mark.parametrize(
#     "input,recieve,date_dir,want",
#     [
#         (
#             "tests/testdata/test_001.json",
#             "data/2021/11/01/test_001.json",
#             "2021/11/01",
#             {
#                 "segment_definitions": [
#                     "scatter/2021/11/01/job_000.pkl",
#                     "scatter/2021/11/01/job_001.pkl",
#                 ]
#             },
#         ),
#     ],
# )
# def test_main(segment_handle, input, recieve, date_dir, want):
#     s3_upload(segment_handle.s3, input, segment_handle.aggregate_bucket, recieve)
#     segment_handle.recieve = recieve
#     segment_handle.date_dir = date_dir
#     assert segment_handle.main() == want
