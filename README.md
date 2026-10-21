# ДЗ 3 «Инженерия данных»: RustFS + свой MapReduce + DuckDB

**Вариант 10 — Аналитики** (IT-профроли hh.ru: Аналитик, Системный, Бизнес-, BI-/аналитик данных, Продуктовый).
Голубева Мария Сергеевна, J3313.

📄 Отчёт: [`report/report.pdf`](report/report.pdf)

## Структура
| файл | этап |
|---|---|
| `docker-compose.yml` | 1. RustFS в Docker, 4 диска, EC:2 |
| `00_prepare.py` | 2. срез вакансий «*аналит*» из выгрузки hh.ru за 2024 (25 ГБ), без дублей |
| `01_upload.py` | 2. бакет `hh-lake`, объект `raw/hh_analysts_2024.csv` |
| `mapreduce.py` | 3. свой MapReduce: Range-чтение из S3 чанками, map+combiner, shuffle по crc32, reduce, перезапуск упавшего маппера (`--fail-chunk N`) |
| `duckdb_skills.py` | 3. то же на SQL в DuckDB поверх S3 (httpfs), режимы `opt` / `naive` |
| `mapreduce_v2.py` | бонус: сплиты читают мапперы, shuffle в маппере, spill на диск, параллельные редьюсеры (в 2,8 раза быстрее) |
| `verify.py` | 4. сверка результатов двух способов из бакета (`--a`, `--b`) |
| `salary.py` | 5. пары и тройки, повышающие/понижающие медианную зарплату |
| `regions.py` | 6. Москва / Санкт-Петербург / Екатеринбург |
| `profile_runs.py` | 7. профилирование: 3 запуска, время и пик RSS; «до/после» |
| `make_charts.py`, `build_report.py`, `report_template.html` | графики и сборка отчёта |
| `common.py` | настройки S3, условие варианта, нормализация навыков, `put_bytes` (multipart) |
| `s3_test.py`, `s3_test2.py` | диагностика сбоя PutObject в RustFS |
| `logs/` | выводы всех запусков; `results/` — результаты (k3 — только в бакете) |

## Запуск
```bash
docker compose up -d                     # из папки с docker-compose.yml
python3 -m venv ~/venvs/hw3 && source ~/venvs/hw3/bin/activate
pip install -r requirements.txt
python 00_prepare.py && python 01_upload.py
python mapreduce.py && python mapreduce.py --fail-chunk 5 --no-save
python mapreduce_v2.py && python mapreduce_v2.py --fail-split 5 --no-save
python duckdb_skills.py && python verify.py && python verify.py --a mapreduce_v2 --b duckdb
python salary.py && python regions.py
python profile_runs.py
python make_charts.py && python build_report.py
```

## Бонус: MapReduce v2 (`mapreduce_v2.py`)
Профилирование v1 по фазам показало, что параллельна только фаза map (~1,2 с), а чтение объекта (~3 с)
и shuffle 2,7 млн ключей (~3 с) идут в одном главном процессе — по закону Амдала это и ограничивает ускорение.
В v2:
* **сплиты как в Hadoop** — драйвер только делит объект на диапазоны байтов, каждый маппер сам читает
  свой диапазон из S3; начало записи находится по шаблону `\nhh,<vacancy_id>,`
  (73 522 совпадения = 73 522 записи), хвост последней записи дочитывается из следующего сплита;
* **shuffle в маппере** — combiner + раскладка ключей по 4 партициям по `crc32`;
* **spill на диск** — выход маппера по партициям пишется в локальные файлы, а не идёт через главный процесс;
* **параллельные редьюсеры** — каждый сливает свою партицию и сам пишет `results/mapreduce_v2/k*/part-0000N.csv` в бакет;
* отказ маппера: `python mapreduce_v2.py --fail-split 5 --no-save` (лог `logs/09_mapreduce_v2_fail.txt`);
* сверка с DuckDB: `python verify.py --a mapreduce_v2 --b duckdb` — полное совпадение (`logs/10_verify_v2.txt`).

| вариант (3 запуска) | время, с | пик памяти, МБ |
|---|---|---|
| MR v1 baseline (1 воркер, 1 МБ) | 9,80 ± 0,24 | 567 |
| MR v1 (4 воркера, 8 МБ) | 9,04 ± 0,66 | 865 |
| MR v1 (4 воркера, 32 МБ) | 8,13 ± 0,11 | 1 266 |
| **MR v2 (4 воркера, 8 МБ)** | **3,50 ± 0,05** | 953 |
| DuckDB naive | 12,57 ± 0,39 | 1 785 |
| **DuckDB opt** | **5,23 ± 0,07** | 1 839 |
