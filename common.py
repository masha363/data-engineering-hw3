"""Общие настройки ДЗ 3: хранилище, вариант, нормализация навыков."""
import os
import boto3
from botocore.config import Config

# --- Хранилище RustFS (поднято в ДЗ 2, docker-compose.yml в ../) ---
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.environ.get("S3_KEY", "rustfsadmin")
S3_SECRET = os.environ.get("S3_SECRET", "rustfsadmin")
BUCKET = "hh-lake"
RAW_KEY = "raw/hh_analysts_2024.csv"      # объект с вакансиями сегмента «аналитики»
RESULTS_PREFIX = "results"                # сюда кладём результаты

# --- Вариант 10: аналитики (IT-аналитики по профроли hh.ru) ---
VARIANT = "10. Аналитики"
VARIANT_ROLES = (
    "Аналитик",
    "BI-аналитик, аналитик данных",
    "Бизнес-аналитик",
    "Системный аналитик",
    "Продуктовый аналитик",
)
VARIANT_SQL = "role IN (" + ", ".join("'" + r + "'" for r in VARIANT_ROLES) + ")"

# Регионы для этапа 6
REGIONS = ("Москва", "Санкт-Петербург", "Екатеринбург")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3},
                      # новые boto3 по умолчанию шлют CRC-чексуммы в trailer-заголовках,
                      # RustFS на put_object отвечает на это InternalError -> только когда нужно
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def parse_skills(raw):
    """'Python; SQL;  Excel' -> отсортированный список уникальных навыков.
    То же правило, что в DuckDB: split по ';', trim пробелов, без пустых, distinct, sort."""
    if not raw:
        return []
    return sorted({s.strip(" ") for s in raw.split(";")} - {""})


def duckdb_s3_setup(con):
    """Подключаем DuckDB к RustFS через httpfs."""
    host = S3_ENDPOINT.split("://", 1)[1]
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET rustfs (
            TYPE s3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}',
            ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')
    """)


def read_csv_sql(path):
    return (f"read_csv('{path}', header=true, all_varchar=true, quote='\"', escape='\"', "
            f"delim=',', max_line_size=20000000)")


SALARY_SQL = """
    CASE WHEN currency = 'RUR' THEN
        (CASE WHEN try_cast(salary_from AS DOUBLE) IS NOT NULL AND try_cast(salary_to AS DOUBLE) IS NOT NULL
              THEN (try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2
              ELSE coalesce(try_cast(salary_from AS DOUBLE), try_cast(salary_to AS DOUBLE)) END)
        * (CASE WHEN gross = 'true' THEN 0.87 ELSE 1 END)
    END"""
# зарплата «на руки» в рублях: середина вилки (или единственная граница), gross -> минус 13% НДФЛ;
# выбросы отсекаем: 15 тыс. - 1.5 млн ₽
SALARY_MIN, SALARY_MAX = 15_000, 1_500_000


def load_variant(con):
    """Таблица v (вакансии варианта с навыками и зарплатой) и e (вакансия-навык)."""
    from_csv = read_csv_sql(f"s3://{BUCKET}/{RAW_KEY}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v AS
        SELECT vacancy_id AS rid, area, role, experience, schedule, employer_id, employer_name, name,
               list_distinct(list_filter(
                   list_transform(string_split(coalesce(key_skills, ''), ';'), x -> trim(x)),
                   x -> x <> '')) AS skills,
               CASE WHEN s BETWEEN {SALARY_MIN} AND {SALARY_MAX} THEN s END AS salary
        FROM (SELECT *, {SALARY_SQL} AS s FROM {from_csv} WHERE {VARIANT_SQL})
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE e AS SELECT rid, unnest(skills) AS s FROM v")


def print_table(con, sql, title=None):
    """Печатает результат запроса ровной таблицей (числа — справа, с пробелами-разделителями)."""
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    def f(x):
        if x is None:
            return "-"
        if isinstance(x, float):
            return f"{x:,.1f}".replace(",", " ") if abs(x) < 100 else f"{x:,.0f}".replace(",", " ")
        if isinstance(x, int):
            return f"{x:,}".replace(",", " ")
        return str(x)

    num = [all(isinstance(r[i], (int, float)) or r[i] is None for r in rows) for i in range(len(cols))]
    data = [[f(x) for x in r] for r in rows]
    w = [max([len(c)] + [len(r[i]) for r in data]) for i, c in enumerate(cols)]
    if title:
        print(f"\n### {title}")
    print("  ".join(c.rjust(w[i]) if num[i] else c.ljust(w[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * x for x in w))
    for r in data:
        print("  ".join(r[i].rjust(w[i]) if num[i] else r[i].ljust(w[i]) for i in range(len(cols))))
    return cols, rows


def put_bytes(s3, key, body):
    """Запись объекта в RustFS всегда через multipart upload.
    В нашей сборке RustFS (v1.0.3, EC:2 на 4 дисках) обычный PutObject объектов
    крупнее ~256 КБ падает с InternalError (мелкие хранятся inline и проходят),
    а multipart работает стабильно — им же была загружена исходная выгрузка."""
    import io
    from boto3.s3.transfer import TransferConfig
    cfg = TransferConfig(multipart_threshold=1, multipart_chunksize=8 * 2**20, use_threads=False)
    s3.upload_fileobj(io.BytesIO(body), BUCKET, key, Config=cfg)
