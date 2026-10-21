"""Собирает report/report.html из шаблона, логов, скриншотов и графиков,
затем (если есть Google Chrome) печатает его в report/report.pdf."""
import html
import os
import re
import subprocess

import duckdb
import boto3

T = open("report_template.html", encoding="utf-8").read()


def log(m):
    name = m.group(1)
    path = f"logs/{name}.txt"
    if not os.path.exists(path):
        return f'<p class="miss">нет файла {path}</p>'
    txt = open(path, encoding="utf-8").read().rstrip()
    return f'<div class="cap">Вывод: <code>{path}</code></div><pre>{html.escape(txt)}</pre>'


def img(m):
    name, cap = m.group(1), m.group(2)
    for ext in ("png", "jpg", "jpeg"):
        p = f"screenshots/{name}.{ext}"
        if os.path.exists(p):
            return f'<figure><img class="shot" src="../{p}"><figcaption>{cap}</figcaption></figure>'
    return ""          # скриншота нет — в отчёте остаётся текстовый вывод из logs/


def chart(m):
    return f'<figure><img src="{m.group(1)}.png"><figcaption>{m.group(2)}</figcaption></figure>'


def code(m):
    path = m.group(1)
    return f'<div class="cap">Файл <code>{path}</code></div><pre class="code">{html.escape(open(path, encoding="utf-8").read().rstrip())}</pre>'


out = re.sub(r"\{\{log:([\w\-]+)\}\}", log, T)
out = re.sub(r"\{\{img:([\w\-]+)\|([^}]*)\}\}", img, out)
out = re.sub(r"\{\{chart:([\w\-]+)\|([^}]*)\}\}", chart, out)
out = re.sub(r"\{\{code:([\w\-\.]+)\}\}", code, out)
_n = iter(range(1, 1000))
out = re.sub(r"Рис\. \d+\.", lambda m: f"Рис. {next(_n)}.", out)     # сквозная нумерация рисунков
repo = open("repo_url.txt").read().strip() if os.path.exists("repo_url.txt") else "(ссылка будет добавлена)"
out = out.replace("{{repo_url}}", repo)
out = out.replace("{{duckdb_version}}", duckdb.__version__).replace("{{boto3_version}}", boto3.__version__)
open("report/report.html", "w", encoding="utf-8").write(out)
print("report/report.html готов")

chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
if os.path.exists(chrome):
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={os.path.abspath('report/report.pdf')}",
                    "file://" + os.path.abspath("report/report.html")], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("report/report.pdf готов")
else:
    print("Chrome не найден: открой report/report.html в Safari -> Файл -> Экспортировать как PDF")
