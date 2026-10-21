"""Тот же подсчёт навыков, пар и троек — SQL-запросами DuckDB к тому же объекту в RustFS.
--mode opt   : один проход по CSV, в память берём только нужные колонки (по умолчанию)
--mode naive : каждый из трёх запросов заново читает весь CSV из S3 (для сравнения «до/после»)"""
import argparse
import json
import os
import time

import duckdb
from common import put_bytes, s3_client, BUCKET, RAW_KEY, RESULTS_PREFIX, VARIANT_SQL, duckdb_s3_setup, read_csv_sql

SRC = read_csv_sql(f"s3://{BUCKET}/{RAW_KEY}")

# условие варианта + нормализация навыков (то же правило, что parse_skills в Python)
VACANCIES_SQL = f"""
    SELECT vacancy_id AS rid,   -- vacancy_id уникален (дубли убраны на подготовке)
           list_distinct(list_filter(
               list_transform(string_split(coalesce(key_skills, ''), ';'), x -> trim(x)),
               x -> x <> '')) AS skills
    FROM {SRC}
    WHERE {VARIANT_SQL}
"""

K_SQL = {
    1: """SELECT a.s AS skill_1, count(*) AS cnt
          FROM e a GROUP BY ALL""",
    2: """SELECT a.s AS skill_1, b.s AS skill_2, count(*) AS cnt
          FROM e a JOIN e b ON a.rid = b.rid AND a.s < b.s GROUP BY ALL""",
    3: """SELECT a.s AS skill_1, b.s AS skill_2, c.s AS skill_3, count(*) AS cnt
          FROM e a JOIN e b ON a.rid = b.rid AND a.s < b.s
                   JOIN e c ON b.rid = c.rid AND b.s < c.s GROUP BY ALL""",
}
NAMES = {1: "skills", 2: "pairs", 3: "triples"}


def run(mode="opt", save=True, quiet=False, threads=None):
    t0 = time.time()
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads = {threads}")
    duckdb_s3_setup(con)
    log = (lambda m: None) if quiet else (lambda m: print(f"[{time.time() - t0:6.1f} c] {m}", flush=True))
    log("httpfs подключён, читаю CSV из S3")

    if mode == "opt":
        con.execute(f"CREATE TEMP TABLE vs AS {VACANCIES_SQL}")
        con.execute("CREATE TEMP TABLE e AS SELECT rid, unnest(skills) AS s FROM vs")
        log("вакансии варианта загружены")
        prefix = ""
    else:  # naive: e пересчитывается из CSV в каждом запросе
        prefix = f"WITH vs AS MATERIALIZED ({VACANCIES_SQL}), e AS MATERIALIZED (SELECT rid, unnest(skills) AS s FROM vs) "

    tables = {}
    for k in (1, 2, 3):
        q = f"{prefix}{K_SQL[k]}"
        con.execute(f"CREATE TEMP TABLE r{k} AS {q}")
        tables[k] = f"r{k}"
        log(f"k={k} посчитано")

    # контрольные числа
    vs_src = "vs" if mode == "opt" else f"({VACANCIES_SQL})"
    n_rows = con.execute(f"SELECT count(*) FROM {SRC}").fetchone()[0]
    n_var, n_sk = con.execute(f"SELECT count(*), count(*) FILTER (len(skills) > 0) FROM {vs_src}").fetchone()
    hist = dict(con.execute(f"SELECT len(skills), count(*) FROM {vs_src} GROUP BY 1 ORDER BY 1").fetchall())
    distinct, total = [], []
    for k in (1, 2, 3):
        d, t = con.execute(f"SELECT count(*), coalesce(sum(cnt), 0) FROM r{k}").fetchone()
        distinct.append(d); total.append(int(t))
    t_total = time.time() - t0
    summary = {
        "method": "duckdb", "mode": mode,
        "rows_in_object": n_rows, "variant_vacancies": n_var, "with_skills": n_sk,
        "skills_hist": {str(k): v for k, v in hist.items()},
        "distinct": distinct, "total": total, "seconds": round(t_total, 2),
    }

    log("контрольные числа посчитаны")
    if save:
        os.makedirs("results/duckdb", exist_ok=True)
        s3 = s3_client()
        for k in (1, 2, 3):
            cols = ", ".join(f"skill_{i}" for i in range(1, k + 1))
            q = f"SELECT * FROM r{k} ORDER BY cnt DESC, {cols}"
            name = f"k{k}_{NAMES[k]}.csv"
            con.execute(f"COPY ({q}) TO 'results/duckdb/{name}' (HEADER)")
            # httpfs пишет небольшие файлы одним PUT, а такой PUT наш RustFS не принимает
            # (см. put_bytes в common.py) -> кладём в бакет через multipart
            put_bytes(s3, f"{RESULTS_PREFIX}/duckdb/{name}", open(f"results/duckdb/{name}", "rb").read())
            log(f"{name} сохранён в S3 и локально")
        js = json.dumps(summary, ensure_ascii=False, indent=2)
        open("results/duckdb/summary.json", "w").write(js)
        put_bytes(s3, f"{RESULTS_PREFIX}/duckdb/summary.json", js.encode())

    if not quiet:
        print(f"=== DUCKDB ({mode}) | вариант: аналитики ===")
        print(f"строк в объекте: {n_rows}, вакансий варианта: {n_var}, с навыками: {n_sk}")
        for k, name in enumerate(["навыки", "пары", "тройки"]):
            print(f"k={k + 1} {name:<7} уникальных: {distinct[k]:>9}   всего вхождений: {total[k]:>10}")
        for k, name in [(1, "навыков"), (2, "пар"), (3, "троек")]:
            cols = ", ".join(f"skill_{i}" for i in range(1, k + 1))
            print(f"\nТоп-10 {name}:")
            for row in con.execute(f"SELECT * FROM r{k} ORDER BY cnt DESC, {cols} LIMIT 10").fetchall():
                print(f"  {row[-1]:>6}  {' + '.join(row[:-1])}")
        print(f"\n {{'mode': '{mode}', 'seconds': {t_total:.2f}}}")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["opt", "naive"], default="opt")
    ap.add_argument("--no-save", action="store_true")
    a = ap.parse_args()
    run(a.mode, save=not a.no_save)
