"""Диагностика: какие способы записи в RustFS работают через boto3."""
import io, boto3, botocore
from common import s3_client, BUCKET
print("boto3", boto3.__version__, "botocore", botocore.__version__)
s3 = s3_client()
tests = {
    "put_object ascii":   lambda: s3.put_object(Bucket=BUCKET, Key="test/a.txt", Body=b"hello"),
    "put_object utf8":    lambda: s3.put_object(Bucket=BUCKET, Key="test/b.txt", Body="привет".encode()),
    "upload_fileobj":     lambda: s3.upload_fileobj(io.BytesIO(b"hello"), BUCKET, "test/c.txt"),
    "put_object dir key": lambda: s3.put_object(Bucket=BUCKET, Key="results/mapreduce/x.txt", Body=b"hello"),
    "put_object k1 file": lambda: s3.put_object(Bucket=BUCKET, Key="test/k1.csv", Body=open("results/mapreduce/k1_skills.csv","rb").read()),
    "upload_file k1 file": lambda: s3.upload_file("results/mapreduce/k1_skills.csv", BUCKET, "test/k1b.csv"),
}
for name, f in tests.items():
    try:
        f(); print("OK  ", name)
    except Exception as e:
        print("FAIL", name, "->", str(e)[:150])
