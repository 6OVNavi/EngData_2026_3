import json
import re
import threading
import time
from urllib.parse import urlparse

import boto3
import duckdb
import psutil
from botocore.config import Config

S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"

BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
PARQUET_KEY = "raw/it_vacancies_2024.parquet"
RESULTS = "results"

VARIANT_ROLE = "Программист, разработчик"
VARIANT_REGEX = r"\bios\b|android|flutter|react[ -]?native|\bkmp\b|multiplatform|мобильн|\bmobile\b"
VARIANT_EXCLUDE = r"\bqa\b|\baqa\b|тестиров|\btest"

CHUNK_MB = 8
WORKERS = 4
REDUCERS = 4

MIN_PAIR = 20
MIN_TRIPLE = 15
MIN_SKILL = 30

VARIANT_RE = re.compile(VARIANT_REGEX, re.ASCII)
EXCLUDE_RE = re.compile(VARIANT_EXCLUDE, re.ASCII)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def is_variant(name, role):
    name = name.lower()
    return (role == VARIANT_ROLE and VARIANT_RE.search(name) is not None
            and EXCLUDE_RE.search(name) is None)


def parse_skills(raw):
    return sorted({s.strip(" ") for s in raw.split(";")} - {""})


class Profiler:

    def __init__(self, name, step=0.02):
        self.name = name
        self.step = step
        self.peak = 0
        self._stop = False

    def _total_rss(self):
        me = psutil.Process()
        total = me.memory_info().rss
        for ch in me.children(recursive=True):
            try:
                total += ch.memory_info().rss
            except psutil.Error:
                pass
        return total

    def _watch(self):
        while not self._stop:
            self.peak = max(self.peak, self._total_rss())
            time.sleep(self.step)

    def __enter__(self):
        self.t0 = time.perf_counter()
        self.thread = threading.Thread(target=self._watch, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self._stop = True
        self.thread.join()
        self.seconds = time.perf_counter() - self.t0
        res = {"script": self.name, "seconds": round(self.seconds, 2), "peak_mb": round(self.peak / 2**20, 1)}
        print(f"\nВремя: {res['seconds']} с, пик памяти: {res['peak_mb']} МБ")
        print("PROFILE " + json.dumps(res, ensure_ascii=False))


def connect():
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    host = urlparse(S3_ENDPOINT).netloc
    con.sql(f"""
        CREATE SECRET rustfs (
            TYPE s3, KEY_ID '{ACCESS_KEY}', SECRET '{SECRET_KEY}',
            ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'
        )""")
    return con


def source(fmt="csv"):
    if fmt == "parquet":
        return f"read_parquet('s3://{BUCKET}/{PARQUET_KEY}')"
    return f"read_csv('s3://{BUCKET}/{RAW_KEY}', header=true, all_varchar=true)"


def create_mobile(con, fmt="csv"):
    con.sql(f"""
        CREATE OR REPLACE TABLE mobile AS
        SELECT
            row_number() OVER () AS id, area,
            list_sort(list_distinct(list_filter(
                list_transform(string_split(coalesce(key_skills, ''), ';'), x -> trim(x, ' ')),
                x -> x <> ''))) AS skills,
            CASE WHEN currency = 'RUR' THEN coalesce(
                (try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2,
                try_cast(salary_from AS DOUBLE), try_cast(salary_to AS DOUBLE)) END AS salary
        FROM {source(fmt)}
        WHERE role = '{VARIANT_ROLE}'
          AND regexp_matches(lower(name), '{VARIANT_REGEX}')
          AND NOT regexp_matches(lower(name), '{VARIANT_EXCLUDE}')
    """)
    con.sql("""
        CREATE OR REPLACE TABLE mobile_skill AS
        SELECT id, unnest(skills) AS skill, salary, area FROM mobile
    """)


def show(con, sql, title=None):
    rel = con.sql(sql)
    cols, rows = rel.columns, rel.fetchall()
    cells = [[fmt(v) for v in r] for r in rows]
    width = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(cols)]
    if title:
        print(f"\n{title}")
    print(" | ".join(c.ljust(w) for c, w in zip(cols, width)))
    print("-+-".join("-" * w for w in width))
    for r in cells:
        print(" | ".join(v.ljust(w) for v, w in zip(r, width)))
    return cols, rows


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:,.0f}".replace(",", " ") if abs(v) >= 100 or v.is_integer() else f"{v:.1f}"
    if isinstance(v, list):
        return ", ".join(map(str, v))
    return str(v)
