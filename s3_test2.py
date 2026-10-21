"""Диагностика 2: зависит ли ошибка от размера или от содержимого."""
import os
from common import s3_client, BUCKET, put_bytes
s3 = s3_client()
k1 = open("results/mapreduce/k1_skills.csv", "rb").read()
cases = {
    "ascii 100 KB": b"a" * 100_000,
    "ascii 200 KB": b"a" * 200_000,
    "ascii 400 KB": b"a" * 400_000,
    "random 400 KB": os.urandom(400_000),
    "ascii 4 MB": b"a" * 4_000_000,
    "k1 first 50 KB": k1[:50_000],
    "k1 first 150 KB": k1[:150_000],
    "k1 full": k1,
}
for name, body in cases.items():
    try:
        put_bytes(s3, f"test/mp_{name.replace(' ', '_')}", body)
        print("OK   multipart", name)
    except Exception as e:
        print("FAIL", name, "->", str(e)[:90])
