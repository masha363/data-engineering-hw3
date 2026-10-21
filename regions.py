"""Этап 6: сравнение трёх регионов (Москва, Санкт-Петербург, Екатеринбург) для вакансий варианта."""
import os
import time

import duckdb
from common import BUCKET, REGIONS, RESULTS_PREFIX, duckdb_s3_setup, load_variant, print_table, put_bytes, s3_client

t0 = time.time()
con = duckdb.connect()
duckdb_s3_setup(con)
load_variant(con)
R = ", ".join(f"'{r}'" for r in REGIONS)
ORDER = "CASE area " + " ".join(f"WHEN '{r}' THEN {i}" for i, r in enumerate(REGIONS)) + " END"
con.execute(f"CREATE TEMP TABLE rv AS SELECT * FROM v WHERE area IN ({R})")
con.execute("CREATE TEMP TABLE re AS SELECT rv.rid, rv.area, rv.salary, unnest(rv.skills) AS s FROM rv")

print_table(con, f"""SELECT area AS region, count(*) AS vacancies FROM v GROUP BY 1 ORDER BY 2 DESC LIMIT 8""",
            "Топ регионов по числу вакансий аналитиков (выбраны первые три)")

# ---------- Таблица 1: ключевые показатели
con.execute(f"""CREATE TEMP TABLE t1 AS SELECT
        area AS region,
        count(*) AS vacancies,
        round(100.0 * count(*) / (SELECT count(*) FROM v), 1) AS pct_of_russia,
        round(100.0 * count(salary) / count(*), 1) AS pct_with_salary,
        quantile_cont(salary, 0.25) AS p25,
        median(salary) AS median,
        quantile_cont(salary, 0.75) AS p75,
        round(100.0 * count(*) FILTER (schedule = 'Удаленная работа') / count(*), 1) AS pct_remote,
        round(100.0 * count(*) FILTER (experience = 'Нет опыта') / count(*), 1) AS pct_no_exp,
        round(100.0 * count(*) FILTER (len(skills) > 0) / count(*), 1) AS pct_with_skills,
        round(avg(len(skills)) FILTER (len(skills) > 0), 1) AS avg_skills
    FROM rv GROUP BY area ORDER BY {ORDER}""")
print_table(con, "SELECT * FROM t1", "Таблица 1. Ключевые показатели регионов (зарплата на руки, ₽)")

print_table(con, f"""
    SELECT * FROM (PIVOT (SELECT area, role, count(*) AS n FROM rv GROUP BY ALL)
    ON area IN ({R}) USING sum(n) GROUP BY role) ORDER BY "{REGIONS[0]}" DESC""",
            "Структура по ролям (число вакансий)")

print_table(con, f"""
    SELECT * FROM (PIVOT (SELECT area, experience, median(salary) AS m FROM rv WHERE experience IS NOT NULL GROUP BY ALL)
    ON area IN ({R}) USING first(m) GROUP BY experience)
    ORDER BY CASE experience WHEN 'Нет опыта' THEN 0 WHEN 'От 1 года до 3 лет' THEN 1
                             WHEN 'От 3 до 6 лет' THEN 2 ELSE 3 END""",
            "Медианная зарплата по опыту")

# ---------- Таблица 2: востребованность навыков, пар, троек
con.execute(f"""CREATE TEMP TABLE t2 AS
    WITH tot AS (SELECT area, count(*) FILTER (len(skills) > 0) AS n FROM rv GROUP BY area),
    k1 AS (SELECT area, s AS combo, 1 AS k, count(*) AS cnt, median(salary) AS med FROM re GROUP BY ALL),
    k2 AS (SELECT a.area, a.s || ' + ' || b.s AS combo, 2 AS k, count(*) AS cnt, median(a.salary) AS med
           FROM re a JOIN re b ON a.rid = b.rid AND a.s < b.s GROUP BY ALL),
    k3 AS (SELECT a.area, a.s || ' + ' || b.s || ' + ' || c.s AS combo, 3 AS k, count(*) AS cnt, median(a.salary) AS med
           FROM re a JOIN re b ON a.rid = b.rid AND a.s < b.s JOIN re c ON b.rid = c.rid AND b.s < c.s GROUP BY ALL),
    allk AS (SELECT * FROM k1 UNION ALL SELECT * FROM k2 UNION ALL SELECT * FROM k3)
    SELECT area AS region, k, combo, cnt, round(100.0 * cnt / tot.n, 1) AS pct_of_vac_with_skills, med AS median_salary,
           row_number() OVER (PARTITION BY area, k ORDER BY cnt DESC, combo) AS rnk
    FROM allk JOIN tot USING (area)""")
for k, name in [(1, "навыков"), (2, "пар"), (3, "троек")]:
    for r in REGIONS:
        print_table(con, f"""SELECT rnk, combo, cnt, pct_of_vac_with_skills AS pct, median_salary
                             FROM t2 WHERE region = '{r}' AND k = {k} AND rnk <= 10 ORDER BY rnk""",
                    f"Таблица 2.{k}. Топ-10 {name}: {r} (доля от вакансий с навыками, %)")

# ---------- Сравнение долей: чем регионы отличаются друг от друга
print_table(con, f"""
    SELECT * FROM (PIVOT (SELECT region, combo AS skill, pct_of_vac_with_skills AS p FROM t2
                          WHERE k = 1 AND combo IN (SELECT combo FROM t2 WHERE k = 1 GROUP BY combo
                                                    ORDER BY sum(cnt) DESC LIMIT 15))
    ON region IN ({R}) USING first(p) GROUP BY skill) ORDER BY "{REGIONS[0]}" DESC""",
            "Доля вакансий с навыком, % (топ-15 навыков трёх регионов)")

print_table(con, f"""
    WITH s AS (SELECT region, combo AS skill, cnt, pct_of_vac_with_skills AS p FROM t2 WHERE k = 1),
    other AS (SELECT skill, region, p,
                     (sum(cnt) OVER (PARTITION BY skill) - cnt) AS cnt_other FROM s),
    tot AS (SELECT area AS region, count(*) FILTER (len(skills) > 0) AS n FROM rv GROUP BY area)
    SELECT o.region, o.skill, o.p AS pct_here,
           round(100.0 * o.cnt_other / ((SELECT sum(n) FROM tot) - t.n), 1) AS pct_other_two,
           round(o.p / nullif(100.0 * o.cnt_other / ((SELECT sum(n) FROM tot) - t.n), 0), 2) AS lift
    FROM other o JOIN tot t USING (region)
    WHERE o.p >= 3
    QUALIFY row_number() OVER (PARTITION BY o.region ORDER BY lift DESC NULLS LAST) <= 5
    ORDER BY {ORDER.replace('area', 'o.region')}, lift DESC""",
            "Характерные навыки региона: доля здесь / доля в двух других (lift), навыки с долей ≥3%")

os.makedirs("results/analysis", exist_ok=True)
s3 = s3_client()
for t in ("t1", "t2"):
    path = f"results/analysis/regions_{t}.csv"
    con.execute(f"COPY (SELECT * FROM {t} {'WHERE rnk <= 30' if t == 't2' else ''}) TO '{path}' (HEADER)")
    put_bytes(s3, f"{RESULTS_PREFIX}/analysis/regions_{t}.csv", open(path, "rb").read())
print(f"\nСохранено в s3://{BUCKET}/{RESULTS_PREFIX}/analysis/  за {time.time() - t0:.1f} с")
