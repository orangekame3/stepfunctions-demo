import datetime
import json
import subprocess
from random import randint

from fire import Fire


def dummy_data(i):
    id = str(i)
    seed = randint(2, 5)
    date = datetime.date(randint(2019, 2021), randint(1, 12), randint(1, 28))
    lst = []
    for i in range(1, seed):
        if seed > 2:
            date1 = (date + datetime.timedelta(days=i * 15)).strftime("%Y-%m-%d")
            date2 = "xxxx-xx-xx"
            status1 = "接種"
            status2 = "未接種"
        else:
            date1 = "xxxx-xx-xx"
            date2 = (date + datetime.timedelta(days=i * 15)).strftime("%Y-%m-%d")
            status1 = "未接種"
            status2 = "接種"
        lst.append(
            {
                "モ○ルナ": {"接種日": date1, "状態": status1},
                "ファ○ザー": {"接種日": date2, "状態": status2},
            }
        )
    dummy_dict = {
        "社員番号": id,
        "接種記録": lst,
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
            "aws --endpoint-url=http://localhost:4566 --profile localstack s3api create-bucket --bucket aggregatebucket",
        ],
        shell=True,
    )
    subprocess.run(
        [
            "aws s3 --endpoint-url=http://localhost:4566 cp sample.json s3://aggregatebucket/data/ --profile=localstack",
        ],
        shell=True,
    )
    return "created"


if __name__ == "__main__":
    Fire(main)
