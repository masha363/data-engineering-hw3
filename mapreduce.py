"""Собственный MapReduce: читает объект из RustFS частями (HTTP Range),
считает частоты навыков, пар и троек для вакансий варианта.

  driver  : режет объект на чанки по границам записей CSV
  map     : парсит чанк, фильтрует вариант, считает сочетания (+combiner)
  shuffle : раскладывает ключи по R редьюсерам по crc32(ключа)
  reduce  : суммирует счётчики своей партиции
"""
import argparse
import csv
import io
import json
import os
import time
import zlib
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, FIRST_COMPLETED, wait
from itertools import combinations

from common import put_bytes, s3_client, BUCKET, RAW_KEY, RESULTS_PREFIX, VARIANT_ROLES, parse_skills

ROLES = frozenset(VARIANT_ROLES)


# ---------------------------------------------------------------- driver: чтение чанками
def iter_chunks(s3, key, chunk_bytes):
    """Читает объект кусками по chunk_bytes через Range-запросы.
    Режем только по '\\n', перед которым чётное число кавычек (т.е. не внутри
    поля в кавычках): в CSV описания вакансий бывают многострочными."""
    size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    pos, carry, cid = 0, b"", 0
    while pos < size:
        end = min(pos + chunk_bytes, size) - 1
        data = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes={pos}-{end}")["Body"].read()
        pos = end + 1
        buf = carry + data
        if pos >= size:
            carry = b""
            if buf:
                yield cid, buf
            break
        q_total = buf.count(b'"')
        cut = buf.rfind(b"\n")
        while cut != -1 and (q_total - buf.count(b'"', cut)) % 2:
            cut = buf.rfind(b"\n", 0, cut)
        if cut == -1:              # запись длиннее чанка — копим дальше
            carry = buf
            continue
        yield cid, buf[:cut + 1]
        cid += 1
        carry = buf[cut + 1:]
    return size


# ---------------------------------------------------------------- map
def map_chunk(args):
    cid, data, header, attempt, fail_chunk = args
    if fail_chunk is not None and cid == fail_chunk and attempt == 0:
        raise RuntimeError(f"имитация отказа маппера на чанке {cid}")
    i_role = header.index("role")
    i_sk = header.index("key_skills")
    rows = csv.reader(io.StringIO(data.decode("utf-8"), newline=""))
    c1, c2, c3 = Counter(), Counter(), Counter()   # combiner: локальная свёртка
    hist = Counter()                                # распределение числа навыков
    n_rows = n_var = 0
    for r in rows:
        if r == header:
            continue
        n_rows += 1
        if r[i_role] not in ROLES:
            continue
        n_var += 1
        sk = parse_skills(r[i_sk])
        hist[len(sk)] += 1
        c1.update(sk)
        if len(sk) >= 2:
            c2.update(combinations(sk, 2))
        if len(sk) >= 3:
            c3.update(combinations(sk, 3))
    return cid, n_rows, n_var, hist, (c1, c2, c3)


# ---------------------------------------------------------------- shuffle + reduce
def part_of(key, r):
    k = key if isinstance(key, str) else "\x1f".join(key)
    return zlib.crc32(k.encode()) % r


def run(chunk_mb=8, workers=4, reducers=4, fail_chunk=None, save=True, quiet=False):
    t0 = time.time()
    s3 = s3_client()
    chunk_bytes = int(chunk_mb * 2**20)
    parts = [[Counter() for _ in range(reducers)] for _ in range(3)]   # [k][reducer]
    hist = Counter()
    n_rows = n_var = n_chunks = n_retries = 0
    header = None

    def shuffle(res):
        nonlocal n_rows, n_var
        _, nr, nv, h, cs = res
        n_rows += nr; n_var += nv; hist.update(h)
        for k, c in enumerate(cs):
            for key, v in c.items():
                parts[k][part_of(key, reducers)][key] += v

    with ProcessPoolExecutor(max_workers=workers) as ex:
        inflight = {}
        gen = iter_chunks(s3, RAW_KEY, chunk_bytes)
        for cid, data in gen:
            if header is None:                       # первая строка — заголовок
                first_nl = data.index(b"\n")
                header = next(csv.reader([data[:first_nl].decode("utf-8")]))
            n_chunks += 1
            fut = ex.submit(map_chunk, (cid, data, header, 0, fail_chunk))
            inflight[fut] = (cid, data, 0)
            while len(inflight) >= workers * 2:      # ограничиваем память
                n_retries += _drain(ex, inflight, header, fail_chunk, shuffle, quiet, FIRST_COMPLETED)
        while inflight:
            n_retries += _drain(ex, inflight, header, fail_chunk, shuffle, quiet, FIRST_COMPLETED)
    t_map = time.time() - t0

    # reduce: партиции не пересекаются по ключам -> просто объединяем
    result = []
    for k in range(3):
        merged = Counter()
        for p in parts[k]:
            merged.update(p)
        result.append(merged)
    t_total = time.time() - t0

    summary = summarize(result, hist, n_rows, n_var)
    summary.update(method="mapreduce", chunk_mb=chunk_mb, workers=workers, reducers=reducers,
                   chunks=n_chunks, retries=n_retries, seconds=round(t_total, 2), map_seconds=round(t_map, 2))
    if save:
        save_results(s3, "mapreduce", result, summary)
    if not quiet:
        print_summary(summary, result)
    return summary, result


def _drain(ex, inflight, header, fail_chunk, shuffle, quiet, mode):
    done, _ = wait(list(inflight), return_when=mode)
    retries = 0
    for f in done:
        cid, data, attempt = inflight.pop(f)
        try:
            shuffle(f.result())
        except RuntimeError as e:
            retries += 1
            if not quiet:
                print(f"[!] маппер упал: {e} -> перезапуск чанка {cid} (попытка {attempt + 2})")
            nf = ex.submit(map_chunk, (cid, data, header, attempt + 1, fail_chunk))
            inflight[nf] = (cid, data, attempt + 1)
    return retries


# ---------------------------------------------------------------- вывод и сохранение
def summarize(result, hist, n_rows, n_var):
    from math import comb
    exp2 = sum(comb(n, 2) * c for n, c in hist.items())
    exp3 = sum(comb(n, 3) * c for n, c in hist.items())
    return {
        "rows_in_object": n_rows,
        "variant_vacancies": n_var,
        "with_skills": sum(c for n, c in hist.items() if n > 0),
        "skills_hist": {str(k): v for k, v in sorted(hist.items())},
        "distinct": [len(r) for r in result],
        "total": [sum(r.values()) for r in result],
        "expected_total_pairs_sum_C(n,2)": exp2,
        "expected_total_triples_sum_C(n,3)": exp3,
    }


def to_rows(counter):
    rows = []
    for key, v in counter.items():
        key = (key,) if isinstance(key, str) else key
        rows.append((*key, v))
    rows.sort(key=lambda r: (-r[-1], r[:-1]))
    return rows


def save_results(s3, method, result, summary):
    os.makedirs(f"results/{method}", exist_ok=True)
    names = ["skills", "pairs", "triples"]
    for k, (name, counter) in enumerate(zip(names, result), start=1):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([f"skill_{i}" for i in range(1, k + 1)] + ["cnt"])
        w.writerows(to_rows(counter))
        body = buf.getvalue().encode("utf-8")
        key = f"{RESULTS_PREFIX}/{method}/k{k}_{name}.csv"
        open(f"results/{method}/k{k}_{name}.csv", "wb").write(body)
        put_bytes(s3, key, body)
    js = json.dumps(summary, ensure_ascii=False, indent=2).encode()
    open(f"results/{method}/summary.json", "wb").write(js)
    put_bytes(s3, f"{RESULTS_PREFIX}/{method}/summary.json", js)


def print_summary(s, result):
    print(f"=== {s['method'].upper()} | вариант: аналитики ===")
    print(f"строк в объекте: {s['rows_in_object']}, вакансий варианта: {s['variant_vacancies']}, "
          f"с навыками: {s['with_skills']}")
    for k, name in enumerate(["навыки", "пары", "тройки"]):
        print(f"k={k + 1} {name:<7} уникальных: {s['distinct'][k]:>9}   всего вхождений: {s['total'][k]:>10}")
    print(f"проверка: Σ C(n,2) = {s['expected_total_pairs_sum_C(n,2)']}, Σ C(n,3) = {s['expected_total_triples_sum_C(n,3)']}")
    for k, name in enumerate(["навыков", "пар", "троек"]):
        print(f"\nТоп-10 {name}:")
        for r in to_rows(result[k])[:10]:
            print(f"  {r[-1]:>6}  {' + '.join(r[:-1])}")
    extra = {k: s[k] for k in ("chunk_mb", "workers", "chunks", "retries", "seconds") if k in s}
    print("\n", extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-mb", type=float, default=8)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--fail-chunk", type=int, default=None, help="уронить маппер на этом чанке (1-я попытка)")
    ap.add_argument("--no-save", action="store_true")
    a = ap.parse_args()
    run(a.chunk_mb, a.workers, a.reducers, a.fail_chunk, save=not a.no_save)
