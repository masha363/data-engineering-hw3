"""Профилирование: каждый вариант запускается отдельным процессом N раз (по умолчанию 3);
меряем время (wall clock) и пик памяти — максимум суммарного RSS процесса и всех его
дочерних процессов (у MapReduce это воркеры пула), опрос каждые 20 мс через psutil.

Варианты:
  MR  baseline  : 1 воркер, чанк 1 МБ        («до» оптимизации)
  MR  optimized : 4 воркера, чанк 8 МБ       (основной запуск)
  MR  chunk 32  : 4 воркера, чанк 32 МБ      (влияние размера чанка)
  MR  v2        : маппер сам читает свой сплит из S3, shuffle в маппере, spill на диск,
                  параллельные редьюсеры (mapreduce_v2.py)
  DuckDB naive  : каждый запрос k=1,2,3 заново читает CSV из S3 («до»)
  DuckDB opt    : один проход по CSV, только нужные колонки в памяти («после»)
"""
import argparse
import json
import statistics
import subprocess
import sys
import time

import psutil

VARIANTS = {
    "MR baseline (1 worker, 1 MB)":  [sys.executable, "mapreduce.py", "--workers", "1", "--chunk-mb", "1", "--no-save"],
    "MR optimized (4 workers, 8 MB)": [sys.executable, "mapreduce.py", "--workers", "4", "--chunk-mb", "8", "--no-save"],
    "MR 4 workers, 32 MB":           [sys.executable, "mapreduce.py", "--workers", "4", "--chunk-mb", "32", "--no-save"],
    "MR v2 (4 workers, 8 MB)":       [sys.executable, "mapreduce_v2.py", "--workers", "4", "--split-mb", "8", "--no-save"],
    "DuckDB naive":                  [sys.executable, "duckdb_skills.py", "--mode", "naive", "--no-save"],
    "DuckDB opt":                    [sys.executable, "duckdb_skills.py", "--mode", "opt", "--no-save"],
}


def measure(cmd):
    t0 = time.perf_counter()
    p = psutil.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    peak = 0
    while p.poll() is None:
        try:
            procs = [p] + p.children(recursive=True)
            rss = 0
            for q in procs:
                try:
                    rss += q.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak = max(peak, rss)
        except psutil.NoSuchProcess:
            break
        time.sleep(0.02)
    dt = time.perf_counter() - t0
    if p.returncode != 0:
        raise RuntimeError(f"{cmd} упал:\n{p.stderr.read().decode()[-2000:]}")
    return dt, peak / 2**20


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3)
    a = ap.parse_args()
    print(f"CPU: {psutil.cpu_count(logical=False)} ядер, RAM: {psutil.virtual_memory().total / 2**30:.0f} ГБ, "
          f"Python {sys.version.split()[0]}\n")
    rows = []
    for name, cmd in VARIANTS.items():
        times, mems = [], []
        for i in range(a.runs):
            dt, mb = measure(cmd)
            times.append(dt); mems.append(mb)
            print(f"{name:<32} попытка {i + 1}: {dt:6.2f} с   пик памяти {mb:7.1f} МБ", flush=True)
        rows.append({"variant": name, "times": times, "mems": mems,
                     "time_mean": statistics.mean(times), "time_std": statistics.pstdev(times),
                     "mem_mean": statistics.mean(mems), "mem_max": max(mems)})
        print()
    print(f"{'вариант':<32}{'время, с (ср ± σ)':>20}{'пик памяти ср, МБ':>20}{'макс, МБ':>10}")
    print("-" * 82)
    for r in rows:
        print(f"{r['variant']:<32}{r['time_mean']:>12.2f} ± {r['time_std']:<5.2f}{r['mem_mean']:>20.1f}{r['mem_max']:>10.1f}")
    json.dump(rows, open("results/profile.json", "w"), ensure_ascii=False, indent=2)
    print("\nсохранено в results/profile.json")
