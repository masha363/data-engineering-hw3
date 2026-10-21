"""MapReduce v2 — оптимизированная версия (бонус).

Узкие места v1 (mapreduce.py), найденные профилированием фаз:
  1) драйвер в одном процессе сам скачивает весь объект и пересылает воркерам байты чанков;
  2) shuffle (раскладка ~2,7 млн ключей по партициям) идёт в главном процессе на чистом Python;
  3) слияние партиций — тоже в главном процессе.
По закону Амдала эта последовательная часть и ограничивала ускорение от 4 воркеров.

Что изменено:
  * сплиты как в Hadoop: драйвер только делит объект на диапазоны байтов, каждый маппер САМ
    читает свой диапазон из S3 (Range GET) — чтение идёт параллельно, байты не гоняются через IPC;
  * граница записи внутри сплита: начало строки CSV «\\nhh,<vacancy_id>,» (в объекте каждая запись
    начинается с source=hh и числового id; маппер берёт записи, НАЧАЛО которых лежит в его сплите,
    и дочитывает хвост последней записи из следующего сплита);
  * shuffle перенесён в маппер: он сразу возвращает R партиций своего результата;
  * как в Hadoop, выход маппера «сливается» (spill) на локальный диск файлами по партициям,
    а не идёт через главный процесс; редьюсер сам читает файлы своей партиции, сливает их и
    сам пишет свою часть результата в S3 (part-0000r.csv) — главный процесс получает только
    контрольные числа и топ-10. Через главный процесс больше не проходят миллионы ключей.
"""
import argparse
import csv
import heapq
import io
import os
import pickle
import re
import shutil
import tempfile
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations

from common import BUCKET, RAW_KEY, RESULTS_PREFIX, VARIANT_ROLES, parse_skills, put_bytes, s3_client
import mapreduce as v1

ROLES = frozenset(VARIANT_ROLES)
REC_START = re.compile(rb"\n(?=hh,\d+,)")      # позиция '\n' перед началом записи
_S3 = None


def _s3():
    global _S3
    if _S3 is None:
        _S3 = s3_client()
    return _S3


def _get(start, end):
    return _s3().get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={start}-{end - 1}")["Body"].read()


def map_split(args):
    sid, start, end, size, header, reducers, attempt, fail_split, tmpdir = args
    if fail_split is not None and sid == fail_split and attempt == 0:
        raise RuntimeError(f"имитация отказа маппера на сплите {sid}")
    # читаем с запасом: свой диапазон + хвост следующего, пока не встретим начало записи после end
    lo = max(start - 1, 0)
    hi = min(end + 2**20, size)
    data = _get(lo, hi)
    while True:
        m = REC_START.search(data, end - lo) if end < size else None
        if m or hi >= size:
            break
        more = _get(hi, min(hi + 2**20, size)); hi = min(hi + 2**20, size); data += more
    stop = m.start() + 1 if m else len(data)
    if start == 0:
        first = data.index(b"\n") + 1                        # пропускаем заголовок
    else:
        f = REC_START.search(data, 0)
        first = f.start() + 1 if f and f.start() + 1 < stop else stop
    body = data[first:stop]

    i_role, i_sk = header.index("role"), header.index("key_skills")
    c1, c2, c3 = Counter(), Counter(), Counter()     # combiner (Counter.update — цикл на C)
    hist = Counter()
    n_rows = n_var = 0
    for r in csv.reader(io.StringIO(body.decode("utf-8"), newline="")):
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
    # shuffle на стороне маппера: раскладываем уже свёрнутые ключи по R партициям
    parts = [[{}, {}, {}] for _ in range(reducers)]
    for k, c in enumerate((c1, c2, c3)):
        for key, v in c.items():
            parts[v1.part_of(key, reducers)][k][key] = v
    for p in range(reducers):                         # spill на диск
        with open(os.path.join(tmpdir, f"m{sid:05d}_p{p}.pkl"), "wb") as f:
            pickle.dump(parts[p], f, protocol=pickle.HIGHEST_PROTOCOL)
    return sid, n_rows, n_var, hist


NAMES = ["skills", "pairs", "triples"]


def _order(item):
    key, v = item
    return (-v, (key,) if isinstance(key, str) else key)


def reduce_partition(args):
    pid, tmpdir, nsplits, save = args
    out = [Counter(), Counter(), Counter()]
    for sid in range(nsplits):
        with open(os.path.join(tmpdir, f"m{sid:05d}_p{pid}.pkl"), "rb") as f:
            piece = pickle.load(f)
        for k in range(3):
            out[k].update(piece[k])
    stats = {"distinct": [len(c) for c in out], "total": [sum(c.values()) for c in out],
             "top": [heapq.nsmallest(10, c.items(), key=_order) for c in out]}
    if save:
        s3 = _s3()
        for k in range(3):
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow([f"skill_{i}" for i in range(1, k + 2)] + ["cnt"])
            w.writerows(v1.to_rows(out[k]))
            name = f"k{k + 1}_{NAMES[k]}/part-{pid:05d}.csv"
            os.makedirs(f"results/mapreduce_v2/k{k + 1}_{NAMES[k]}", exist_ok=True)
            body = buf.getvalue().encode("utf-8")
            open(f"results/mapreduce_v2/{name}", "wb").write(body)
            put_bytes(s3, f"{RESULTS_PREFIX}/mapreduce_v2/{name}", body)
    return pid, stats


def run(split_mb=8, workers=4, reducers=4, fail_split=None, save=True, quiet=False):
    t0 = time.time()
    s3 = s3_client()
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    head = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range="bytes=0-65535")["Body"].read()
    header = next(csv.reader([head[:head.index(b"\n")].decode("utf-8")]))
    sb = int(split_mb * 2**20)
    splits = [(i, s, min(s + sb, size)) for i, s in enumerate(range(0, size, sb))]

    n_rows = n_var = retries = 0
    hist = Counter()
    tmpdir = tempfile.mkdtemp(prefix="mr2_")
    try:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            mk = lambda sid, s_, e_, att: ex.submit(map_split, (sid, s_, e_, size, header, reducers, att, fail_split, tmpdir))
            todo = {mk(sid, s_, e_, 0): (sid, s_, e_, 0) for sid, s_, e_ in splits}
            while todo:
                fut = next(iter(todo))
                sid, s_, e_, att = todo.pop(fut)
                try:
                    _, nr, nv, h = fut.result()
                except RuntimeError as err:
                    retries += 1
                    if not quiet:
                        print(f"[!] маппер упал: {err} -> перезапуск сплита {sid} (попытка {att + 2})")
                    todo[mk(sid, s_, e_, att + 1)] = (sid, s_, e_, att + 1)
                    continue
                n_rows += nr; n_var += nv; hist.update(h)
            t_map = time.time() - t0
            reduced = list(ex.map(reduce_partition, [(p, tmpdir, len(splits), save) for p in range(reducers)]))
            t_red = time.time() - t0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    t_total = time.time() - t0
    distinct = [sum(st["distinct"][k] for _, st in reduced) for k in range(3)]
    total = [sum(st["total"][k] for _, st in reduced) for k in range(3)]
    top = [heapq.nsmallest(10, [x for _, st in reduced for x in st["top"][k]], key=_order) for k in range(3)]

    from math import comb
    summary = {
        "rows_in_object": n_rows, "variant_vacancies": n_var,
        "with_skills": sum(c for n, c in hist.items() if n > 0),
        "skills_hist": {str(k): v for k, v in sorted(hist.items())},
        "distinct": distinct, "total": total,
        "expected_total_pairs_sum_C(n,2)": sum(comb(n, 2) * c for n, c in hist.items()),
        "expected_total_triples_sum_C(n,3)": sum(comb(n, 3) * c for n, c in hist.items()),
    }
    summary.update(method="mapreduce_v2", split_mb=split_mb, workers=workers, reducers=reducers,
                   chunks=len(splits), retries=retries, seconds=round(t_total, 2), map_seconds=round(t_map, 2),
                   reduce_seconds=round(t_red - t_map, 2))
    if save:
        import json
        js = json.dumps(summary, ensure_ascii=False, indent=2).encode()
        open("results/mapreduce_v2/summary.json", "wb").write(js)
        put_bytes(s3, f"{RESULTS_PREFIX}/mapreduce_v2/summary.json", js)
    if not quiet:
        res_top = [Counter(dict(t)) for t in top]
        v1.print_summary(summary, res_top)
        print(f"  фазы: map+spill {t_map:.2f} c, reduce {t_red - t_map:.2f} c")
    return summary, top


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--split-mb", type=float, default=8)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--fail-split", type=int, default=None)
    ap.add_argument("--no-save", action="store_true")
    a = ap.parse_args()
    run(a.split_mb, a.workers, a.reducers, a.fail_split, save=not a.no_save)
