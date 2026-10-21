"""Этап 5: как навыки, пары и тройки навыков связаны с медианной зарплатой.

Прибавка считается так, как требует задание:
  пара  (A, B): база — более частый навык пары (A), добавка — B;
                прибавка = медиана(вакансии с A и B) - медиана(вакансии с A)
  тройка(A,B,C): база — самая частая из трёх входящих пар, добавка — третий навык;
                прибавка = медиана(тройка) - медиана(базовая пара)
Частота навыка/пары — по всем вакансиям варианта; медианы — по вакансиям с зарплатой в ₽."""
import argparse
import os
import time

import duckdb
from common import (BUCKET, RESULTS_PREFIX, SALARY_MAX, SALARY_MIN, duckdb_s3_setup, load_variant,
                    print_table, put_bytes, s3_client)

ap = argparse.ArgumentParser()
ap.add_argument("--min-skill", type=int, default=100, help="мин. вакансий с зарплатой для навыка")
ap.add_argument("--min-pair", type=int, default=50, help="мин. вакансий с зарплатой для пары")
ap.add_argument("--min-triple", type=int, default=30, help="мин. вакансий с зарплатой для тройки")
ap.add_argument("--min-employers", type=int, default=10,
                help="мин. разных работодателей у пары/тройки: один работодатель-клон (массовые стажировки "
                     "в 20 городах) иначе забивает весь топ одной и той же медианой")
ap.add_argument("--top", type=int, default=10)
a = ap.parse_args()

t0 = time.time()
con = duckdb.connect()
duckdb_s3_setup(con)
load_variant(con)

# Клоны: один работодатель публикует одну и ту же вакансию (то же название, навыки и вилка) в десятках
# городов. Для анализа зарплат по навыкам такая вакансия должна считаться один раз, иначе она
# «перевешивает» медиану любой пары/тройки, в которую входит. Для регионального анализа клоны не убираем.
con.execute("""CREATE TEMP TABLE vu AS SELECT * FROM v
               QUALIFY row_number() OVER (PARTITION BY coalesce(employer_id, rid), name, list_sort(skills), salary
                                          ORDER BY rid) = 1""")
n_v, n_vu = con.execute("SELECT (SELECT count(*) FROM v), (SELECT count(*) FROM vu)").fetchone()
n_vs, n_vus = con.execute("SELECT (SELECT count(salary) FROM v), (SELECT count(salary) FROM vu)").fetchone()
print(f"Клоны вакансий: всего {n_v}, уникальных публикаций {n_vu} (убрано {n_v - n_vu}); "
      f"с зарплатой {n_vs} -> {n_vus}")
con.execute("""CREATE TEMP TABLE es AS
               SELECT vu.rid, unnest(vu.skills) AS s, vu.salary, vu.employer_id AS emp FROM vu WHERE vu.salary IS NOT NULL""")
con.execute("CREATE TEMP TABLE freq1 AS SELECT s, count(*) AS n_all FROM e GROUP BY s")
con.execute("""CREATE TEMP TABLE freq2 AS SELECT a.s AS s1, b.s AS s2, count(*) AS n_all
               FROM e a JOIN e b ON a.rid = b.rid AND a.s < b.s GROUP BY ALL""")
con.execute("CREATE TEMP TABLE m1 AS SELECT s, count(*) AS n, count(DISTINCT emp) AS emps, median(salary) AS med FROM es GROUP BY s")
con.execute("""CREATE TEMP TABLE m2 AS SELECT a.s AS s1, b.s AS s2, count(*) AS n, count(DISTINCT a.emp) AS emps, median(a.salary) AS med
               FROM es a JOIN es b ON a.rid = b.rid AND a.s < b.s GROUP BY ALL""")
con.execute(f"""CREATE TEMP TABLE m3 AS SELECT a.s AS s1, b.s AS s2, c.s AS s3, count(*) AS n, count(DISTINCT a.emp) AS emps, median(a.salary) AS med
               FROM es a JOIN es b ON a.rid = b.rid AND a.s < b.s
                         JOIN es c ON b.rid = c.rid AND b.s < c.s
               GROUP BY ALL HAVING count(*) >= {a.min_triple} AND count(DISTINCT a.emp) >= {a.min_employers}""")

# ---- общая картина
print(f"Вариант: аналитики. Зарплата: середина вилки в ₽ «на руки» (gross -13%), {SALARY_MIN:,}–{SALARY_MAX:,} ₽".replace(",", " "))
print_table(con, """
    SELECT count(*) AS vacancies, count(salary) AS with_salary,
           round(100.0 * count(salary) / count(*), 1) AS pct_salary,
           count(*) FILTER (len(skills) > 0 AND salary IS NOT NULL) AS salary_and_skills,
           median(salary) AS median_all,
           median(salary) FILTER (len(skills) > 0) AS median_with_skills,
           median(salary) FILTER (len(skills) = 0) AS median_no_skills
    FROM v""", "Общая картина")
MED = con.execute("SELECT median(salary) FROM vu WHERE len(skills) > 0").fetchone()[0]
print(f"\nБазовая медиана для сравнения навыков (уникальные публикации с навыками): {MED:,.0f} ₽".replace(",", " "))
print_table(con, """SELECT role, count(*) AS vacancies, count(salary) AS with_salary, median(salary) AS median
                    FROM v GROUP BY role ORDER BY median DESC""", "Медиана по ролям")

# ---- одиночные навыки
SK = f"""SELECT m1.s AS skill, f.n_all AS freq, m1.n AS n_salary, m1.emps AS employers, m1.med AS median,
                m1.med - {MED} AS delta_vs_all, round(100 * (m1.med / {MED} - 1), 1) AS delta_pct
         FROM m1 JOIN freq1 f USING (s) WHERE m1.n >= {a.min_skill} AND m1.emps >= {a.min_employers}"""
print_table(con, f"{SK} ORDER BY median DESC, skill LIMIT {a.top}",
            f"Навыки с самой высокой медианой (≥{a.min_skill} вакансий с зарплатой, ≥{a.min_employers} работодателей)")
print_table(con, f"{SK} ORDER BY median ASC, skill LIMIT {a.top}", "Навыки с самой низкой медианой")

# ---- пары: база = более частый навык
con.execute(f"""CREATE TEMP TABLE pair_up AS
    WITH p AS (
        SELECT m2.*, f1.n_all AS f1, f2.n_all AS f2 FROM m2
        JOIN freq1 f1 ON f1.s = m2.s1 JOIN freq1 f2 ON f2.s = m2.s2
        WHERE m2.n >= {a.min_pair} AND m2.emps >= {a.min_employers})
    SELECT base, added, p.n AS n_pair, p.emps AS employers, p.med AS median_pair,
           mb.n AS n_base, mb.med AS median_base,
           p.med - mb.med AS uplift, round(100 * (p.med / mb.med - 1), 1) AS uplift_pct
    FROM (SELECT *, CASE WHEN f1 > f2 OR (f1 = f2 AND s1 < s2) THEN s1 ELSE s2 END AS base,
                    CASE WHEN f1 > f2 OR (f1 = f2 AND s1 < s2) THEN s2 ELSE s1 END AS added FROM p) p
    JOIN m1 mb ON mb.s = p.base""")
print_table(con, f"SELECT * FROM pair_up ORDER BY uplift DESC, base, added LIMIT {a.top}",
            f"ПАРЫ, сильнее всего ПОВЫШАЮЩИЕ медиану (база — более частый навык; ≥{a.min_pair} вакансий, ≥{a.min_employers} работодателей)")
print_table(con, f"SELECT * FROM pair_up ORDER BY uplift ASC, base, added LIMIT {a.top}",
            "ПАРЫ, сильнее всего ПОНИЖАЮЩИЕ медиану")

# ---- тройки: база = самая частая из трёх входящих пар
con.execute("""CREATE TEMP TABLE triple_up AS
    WITH t AS (
        SELECT m3.*,
               p12.n_all AS f12, p13.n_all AS f13, p23.n_all AS f23 FROM m3
        JOIN freq2 p12 ON p12.s1 = m3.s1 AND p12.s2 = m3.s2
        JOIN freq2 p13 ON p13.s1 = m3.s1 AND p13.s2 = m3.s3
        JOIN freq2 p23 ON p23.s1 = m3.s2 AND p23.s2 = m3.s3),
    b AS (
        SELECT *, CASE WHEN f12 >= f13 AND f12 >= f23 THEN 12 WHEN f13 >= f23 THEN 13 ELSE 23 END AS bp FROM t)
    SELECT CASE bp WHEN 12 THEN b.s1 || ' + ' || b.s2 WHEN 13 THEN b.s1 || ' + ' || b.s3 ELSE b.s2 || ' + ' || b.s3 END AS base_pair,
           CASE bp WHEN 12 THEN b.s3 WHEN 13 THEN b.s2 ELSE b.s1 END AS added,
           b.n AS n_triple, b.emps AS employers, b.med AS median_triple, mb.n AS n_base, mb.med AS median_base,
           b.med - mb.med AS uplift, round(100 * (b.med / mb.med - 1), 1) AS uplift_pct
    FROM b JOIN m2 mb ON mb.s1 = CASE bp WHEN 23 THEN b.s2 ELSE b.s1 END
                     AND mb.s2 = CASE bp WHEN 12 THEN b.s2 ELSE b.s3 END""")
print_table(con, f"SELECT * FROM triple_up ORDER BY uplift DESC, base_pair, added LIMIT {a.top}",
            f"ТРОЙКИ, сильнее всего ПОВЫШАЮЩИЕ медиану (база — самая частая пара внутри; ≥{a.min_triple} вакансий, ≥{a.min_employers} работодателей)")
print_table(con, f"SELECT * FROM triple_up ORDER BY uplift ASC, base_pair, added LIMIT {a.top}",
            "ТРОЙКИ, сильнее всего ПОНИЖАЮЩИЕ медиану")

# ---- итоговая таблица: какие навыки чаще всего дают прибавку / просадку как «добавка»
con.execute("""CREATE TEMP TABLE added_effect AS
    SELECT added AS skill, count(*) AS combos, median(uplift_pct) AS median_uplift_pct,
           count(*) FILTER (uplift > 0) AS combos_up, count(*) FILTER (uplift < 0) AS combos_down
    FROM (SELECT added, uplift, uplift_pct FROM pair_up UNION ALL SELECT added, uplift, uplift_pct FROM triple_up)
    GROUP BY 1 HAVING count(*) >= 5""")
print_table(con, f"SELECT * FROM added_effect ORDER BY median_uplift_pct DESC LIMIT {a.top}",
            "ИТОГ: навыки, которые как ДОБАВКА к паре/тройке чаще всего ПОВЫШАЮТ зарплату (≥5 сочетаний)")
print_table(con, f"SELECT * FROM added_effect ORDER BY median_uplift_pct ASC LIMIT {a.top}",
            "ИТОГ: навыки, которые как ДОБАВКА чаще всего ПОНИЖАЮТ зарплату")

# ---- сохраняем
os.makedirs("results/analysis", exist_ok=True)
s3 = s3_client()
for t in ("pair_up", "triple_up", "added_effect"):
    path = f"results/analysis/salary_{t}.csv"
    con.execute(f"COPY (SELECT * FROM {t} ORDER BY 1, 2) TO '{path}' (HEADER)")
    put_bytes(s3, f"{RESULTS_PREFIX}/analysis/salary_{t}.csv", open(path, "rb").read())
print(f"\nСохранено в s3://{BUCKET}/{RESULTS_PREFIX}/analysis/  за {time.time() - t0:.1f} с")
