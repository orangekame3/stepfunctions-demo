import datetime
import json
import subprocess
from random import randint
from mimesis import Person
from mimesis.locales import Locale

person = Person(Locale.JA)


from fire import Fire


def dummy_data(i):
    id = str(i).zfill(3)
    seed = randint(2, 5)
    date = datetime.date(2021, randint(1, 12), randint(1, 28))
    lst = []
    for i in range(1, seed):
        date1 = (date + datetime.timedelta(days=i * 3)).strftime("%Y-%m-%d")
        lst.append(
            {
                "デッドリフト": {"実施日": date1, "負荷レベル": randint(1, 5)},
                "スクワット": {"実施日": date1, "負荷レベル": randint(1, 5)},
                "ベンチプレス": {"実施日": date1, "負荷レベル": randint(1, 5)},
            }
        )
    dummy_dict = {
        "会員番号": id,
        "名前": person.full_name(reverse=True),
        "トレーニング履歴": lst,
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
            "aws s3 --endpoint-url=http://localhost:4566 cp data/sample.json s3://aggregatebucket/data/ --profile=localstack",
        ],
        shell=True,
    )
    return "created"


if __name__ == "__main__":
    Fire(main)
