"""Этап 1-2: создаём бакет hh-lake в RustFS и кладём туда вакансии сегмента."""
import hashlib
import os
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from common import s3_client, BUCKET, RAW_KEY, S3_ENDPOINT

LOCAL = "../data/hh_analysts_2024.csv"
s3 = s3_client()
print("Endpoint:", S3_ENDPOINT)

try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"Бакет {BUCKET} уже есть")
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Создан бакет {BUCKET}")

size = os.path.getsize(LOCAL)
h = hashlib.md5()
with open(LOCAL, "rb") as f:
    for b in iter(lambda: f.read(8 * 2**20), b""):
        h.update(b)
md5 = h.hexdigest()
s3.upload_file(LOCAL, BUCKET, RAW_KEY, Config=TransferConfig(multipart_threshold=64 * 2**20))
head = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)
print(f"Загружено s3://{BUCKET}/{RAW_KEY}")
print(f"  локальный размер : {size} байт, md5 {md5}")
print(f"  размер в S3      : {head['ContentLength']} байт")
assert head["ContentLength"] == size, "размеры не совпали!"

print("\nСодержимое бакета:")
for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []):
    print(f"  {o['Key']:<50} {o['Size']:>12} байт  {o['LastModified']:%Y-%m-%d %H:%M:%S}")
