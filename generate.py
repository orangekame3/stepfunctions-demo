import json
import subprocess
from fire import Fire


def dummy_data(i):
    id = str(i)
    dummy_dict = {
        "社員番号": id,
        "出勤状況": [
            {
                "在宅": {"出勤日": "2021-11-01T:15:00:00.000Z"},
                "会社": {"出勤日": "2021-11-01T:15:00:00.000Z"},
            }
        ],
    }
    return dummy_dict


def main(k):
    sample_data = []
    for i in range(k):
        sample_data.append(dummy_data(i))

    with open("sample.json", mode="wt", encoding="utf-8") as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=4)
    subprocess.run(
        [
            "aws --endpoint-url=http://localhost:4566 --profile localstack s3api create-bucket --bucket test-bucket",
        ],
        shell=True,
    )
    subprocess.run(
        [
            "aws s3 --endpoint-url=http://localhost:4566 cp sample.json s3://test-bucket/test-data/ --profile=localstack",
        ],
        shell=True,
    )
    return "created"


if __name__ == "__main__":
    Fire(main)
