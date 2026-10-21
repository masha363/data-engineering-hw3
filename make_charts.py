"""Графики для отчёта (report/*.png) из результатов, лежащих в results/."""
import csv
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"figure.dpi": 150, "font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
C1, C2, C3 = "#2b6cb0", "#dd6b20", "#38a169"

# 1. Рост числа сочетаний
s = json.load(open("results/mapreduce/summary.json"))
fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.4))
x = ["k=1 навыки", "k=2 пары", "k=3 тройки"]
a1.bar([i - 0.2 for i in range(3)], s["distinct"], 0.4, label="уникальных", color=C1)
a1.bar([i + 0.2 for i in range(3)], s["total"], 0.4, label="всего вхождений", color=C2)
a1.set_yscale("log"); a1.set_xticks(range(3), x); a1.legend(frameon=False)
for i in range(3):
    a1.text(i - 0.2, s["distinct"][i], f"{s['distinct'][i]:,}".replace(",", " "), ha="center", va="bottom", fontsize=7)
    a1.text(i + 0.2, s["total"][i], f"{s['total'][i]:,}".replace(",", " "), ha="center", va="bottom", fontsize=7)
a1.set_title("Рост числа сочетаний (лог. шкала)")
h = {int(k): v for k, v in s["skills_hist"].items() if int(k) > 0}
a2.bar(list(h), list(h.values()), color=C1)
a2.set_xlabel("число навыков в вакансии, n"); a2.set_ylabel("вакансий")
a2.set_title("Распределение n (каждая вакансия даёт C(n,2) пар и C(n,3) троек)")
fig.tight_layout(); fig.savefig("report/chart_growth.png"); plt.close(fig)

# 2. Профилирование
p = json.load(open("results/profile.json"))
names = [r["variant"] for r in p]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.4))
cols = [C2 if n.startswith("DuckDB") else (C3 if "v2" in n else C1) for n in names]
a1.barh(names, [r["time_mean"] for r in p], xerr=[r["time_std"] for r in p], color=cols)
a1.invert_yaxis(); a1.set_xlabel("время, с (среднее по 3 запускам ± σ)")
a2.barh(names, [r["mem_mean"] for r in p], color=cols)
a2.invert_yaxis(); a2.set_xlabel("пик памяти, МБ (RSS процесса + дочерних)"); a2.set_yticks([])
for a, key, fmt in ((a1, "time_mean", "{:.1f} с"), (a2, "mem_mean", "{:.0f} МБ")):
    for i, r in enumerate(p):
        a.text(r[key], i, " " + fmt.format(r[key]), va="center", fontsize=8)
fig.suptitle("Профилирование: MapReduce v1 (синий), MapReduce v2 (зелёный), DuckDB (оранжевый)")
fig.tight_layout(); fig.savefig("report/chart_profile.png"); plt.close(fig)

# 3. Регионы: медианы по опыту
exp = ["Нет опыта", "От 1 года до 3 лет", "От 3 до 6 лет", "Более 6 лет"]
# значения из logs/07_regions.txt, таблица «Медианная зарплата по опыту»
reg = {"Москва": [66375, 120000, 210000, 250000],
       "Санкт-Петербург": [60000, 100000, 170000, 235000],
       "Екатеринбург": [55000, 80000, 130250, 170000]}
fig, ax = plt.subplots(figsize=(8, 3.2))
for j, (r, c) in enumerate(zip(reg, (C1, C2, C3))):
    ax.bar([i + (j - 1) * 0.27 for i in range(4)], [v / 1000 for v in reg[r]], 0.27, label=r, color=c)
ax.set_xticks(range(4), exp); ax.set_ylabel("медиана, тыс. ₽ на руки"); ax.legend(frameon=False)
ax.set_title("Медианная зарплата аналитиков по опыту и региону")
fig.tight_layout(); fig.savefig("report/chart_regions.png"); plt.close(fig)

# 4. Пары: прибавка от добавочного навыка
rows = list(csv.DictReader(open("results/analysis/salary_pair_up.csv", encoding="utf-8")))
rows.sort(key=lambda r: float(r["uplift"]))
sel = rows[:8] + rows[-8:]
fig, ax = plt.subplots(figsize=(9, 4.6))
lab = [f"{r['base']}  +  {r['added']}" for r in sel]
val = [float(r["uplift"]) / 1000 for r in sel]
ax.barh(lab, val, color=[C2 if v < 0 else C3 for v in val])
ax.axvline(0, color="#555", lw=0.8)
ax.set_xlabel("прибавка к медиане базового (более частого) навыка, тыс. ₽")
ax.set_title("Пары: что даёт добавочный навык (≥50 вакансий, ≥10 работодателей, без клонов)")
fig.tight_layout(); fig.savefig("report/chart_pairs.png"); plt.close(fig)
print("графики сохранены в report/")
