import datetime
import json
from random import randint
from mimesis import Person
from mimesis.locales import Locale
import boto3
import datetime
import uuid
import shutil
import os

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

    endpoint = f"http://localhost:4566"
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )
    now = datetime.datetime.now()
    uu = str(uuid.uuid4())
    bucket = "aggregatebucket"
    dayfolder = "data/" + now.strftime("%Y/%m/%d")
    send = "data/" + now.strftime("%Y/%m/%d") + "/sample_{0}.json".format(uu)
    print(send)
    sample_data = []
    shutil.rmtree("utils/data/")
    os.makedirs("utils/data/", exist_ok=True)
    for i in range(k):
        sample_data.append(dummy_data(i))

    with open(
        "utils/data/sample_{0}.json".format(uu), mode="wt", encoding="utf-8"
    ) as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=4)

    delete_all_keys_v2(s3, bucket, dayfolder, True)

    s3.create_bucket(
        Bucket=bucket,
    )

    s3.put_object(
        Body=json.dumps(sample_data, ensure_ascii=False, indent=4),
        Bucket=bucket,
        Key=send,
    )
    return "created : {0}".format(send)


# https://dev.classmethod.jp/articles/20180625-how-to-delete-s3folder/
def delete_all_keys_v2(s3, bucket, prefix, dryrun=False):
    contents_count = 0
    next_token = ""

    while True:
        if next_token == "":
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        else:
            response = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=next_token
            )

        if "Contents" in response:
            contents = response["Contents"]
            contents_count = contents_count + len(contents)
            for content in contents:
                if not dryrun:
                    print("Deleting: s3://" + bucket + "/" + content["Key"])
                    s3.delete_object(Bucket=bucket, Key=content["Key"])
                else:
                    print("DryRun: s3://" + bucket + "/" + content["Key"])
                    s3.delete_object(Bucket=bucket, Key=content["Key"])

        if "NextContinuationToken" in response:
            next_token = response["NextContinuationToken"]
        else:
            break


if __name__ == "__main__":

    Fire(main)
