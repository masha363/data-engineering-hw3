"""Этап 4: сверяем результаты MapReduce и DuckDB, взятые из хранилища hh-lake."""
import argparse
import csv
import io
import json
from common import s3_client, BUCKET, RESULTS_PREFIX

ap = argparse.ArgumentParser()
ap.add_argument("--a", default="mapreduce", help="первый способ (папка в results/)")
ap.add_argument("--b", default="duckdb", help="второй способ")
args = ap.parse_args()
A, B = args.a, args.b
s3 = s3_client()
get = lambda key: s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")

print("Объекты результатов в бакете:")
for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=RESULTS_PREFIX + "/").get("Contents", []):
    print(f"  s3://{BUCKET}/{o['Key']:<40} {o['Size']:>10} байт")

sm = json.loads(get(f"{RESULTS_PREFIX}/{A}/summary.json"))
sd = json.loads(get(f"{RESULTS_PREFIX}/{B}/summary.json"))
print(f"\n{'контрольное число':<28}{A:>14}{B:>14}  совпало")
ok_all = True
for key in ("rows_in_object", "variant_vacancies", "with_skills"):
    ok = sm[key] == sd[key]; ok_all &= ok
    print(f"{key:<28}{sm[key]:>14}{sd[key]:>14}  {'да' if ok else 'НЕТ'}")
for k, name in enumerate(["навыки", "пары", "тройки"]):
    for what, label in (("distinct", "уникальных"), ("total", "вхождений")):
        a, b = sm[what][k], sd[what][k]
        ok = a == b; ok_all &= ok
        print(f"{name + ' ' + label:<28}{a:>14}{b:>14}  {'да' if ok else 'НЕТ'}")
ok = sm["skills_hist"] == sd["skills_hist"]; ok_all &= ok
print(f"{'распределение числа навыков':<28}{'':>14}{'':>14}  {'да' if ok else 'НЕТ'}")

print("\nПолное сравнение таблиц (каждая строка и каждое число):")
for k, name in enumerate(["skills", "pairs", "triples"], start=1):
    def load(m):
        """один файл k{k}_{name}.csv или папка с частями редьюсеров k{k}_{name}/part-*.csv"""
        objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{RESULTS_PREFIX}/{m}/k{k}_{name}").get("Contents", [])
        out = {}
        for o in objs:
            for r in list(csv.reader(io.StringIO(get(o["Key"]))))[1:]:
                out[tuple(r[:-1])] = int(r[-1])
        return out
    a, b = load(A), load(B)
    diff = {x for x in a.keys() | b.keys() if a.get(x) != b.get(x)}
    ok_all &= not diff
    print(f"  k={k}: строк {A}={len(a)}, {B}={len(b)}, расхождений: {len(diff)}")
    for x in list(diff)[:5]:
        print("     ", x, a.get(x), b.get(x))
print("\nИТОГ:", "результаты ПОЛНОСТЬЮ СОВПАДАЮТ" if ok_all else "ЕСТЬ РАСХОЖДЕНИЯ")
