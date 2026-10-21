"""Этап 2 (подготовка): из полного выгруза hh.ru за 2024 (25 ГБ) вырезаем
все вакансии с профролью «...аналит...» и убираем дубли по vacancy_id
(оставляем последний снимок). Полный файл в RustFS не помещается на диск
(EC:2 удваивает объём), поэтому в хранилище кладём только этот срез.
Точное условие варианта (5 IT-ролей) применяется уже в MapReduce и DuckDB."""
import time
import duckdb


SRC = "../data/vacancies_2024.csv"
DST = "../data/hh_analysts_2024.csv"

t0 = time.time()
con = duckdb.connect()
src = (
    f"read_csv('{SRC}', header=true, all_varchar=true, quote='\"', escape='\"', "
    f"delim=',', max_line_size=20000000, ignore_errors=true)")

con.execute(f"""
    CREATE TEMP TABLE a AS
    SELECT * FROM {src}
    WHERE lower(role) LIKE '%аналит%'
    QUALIFY row_number() OVER (PARTITION BY vacancy_id ORDER BY snapshot_date DESC, published_at DESC) = 1
""")
print("Вакансий с ролью *аналит* (без дублей):", con.execute("SELECT count(*) FROM a").fetchone()[0])
print(con.execute("SELECT role, count(*) n FROM a GROUP BY role ORDER BY n DESC").fetchall())
con.execute(f"COPY (SELECT * FROM a ORDER BY vacancy_id) TO '{DST}' (HEADER, DELIMITER ',', QUOTE '\"', ESCAPE '\"')")
print(f"Записано в {DST} за {time.time() - t0:.1f} с")
