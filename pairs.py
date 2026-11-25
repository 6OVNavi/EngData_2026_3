import codecs
import csv
import io
import json
import os
import re
import resource
import sys
from collections import Counter
from itertools import combinations

import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
PARTS = int(os.environ.get("PARTS", "4"))
CHUNK = 8 * 2**20

VARIANT_ROLE = "Программист, разработчик"
VARIANT_RE = re.compile(r"\bios\b|android|flutter|react[ -]?native|\bkmp\b|multiplatform|мобильн|\bmobile\b", re.ASCII)
EXCLUDE_RE = re.compile(r"\bqa\b|\baqa\b|тестиров|\btest", re.ASCII)

csv.field_size_limit(2**31 - 1)
s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id="rustfsadmin",
                  aws_secret_access_key="rustfsadmin", region_name="us-east-1",
                  config=Config(s3={"addressing_style": "path"}))


def is_variant(name, role):
    name = name.lower()
    return role == VARIANT_ROLE and VARIANT_RE.search(name) is not None and EXCLUDE_RE.search(name) is None


def parse_skills(raw):
    return sorted({s.strip(" ") for s in raw.split(";")} - {""})


def read_rows(key, state):
    size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    n = (size + CHUNK - 1) // CHUNK
    state["chunks"] = n
    decoder = codecs.getincrementaldecoder("utf-8-sig")()

    def lines():
        tail = ""
        for i in range(n):
            end = min((i + 1) * CHUNK, size) - 1
            body = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes={i * CHUNK}-{end}")["Body"].read()
            state["chunk"] = i
            parts = (tail + decoder.decode(body, final=i == n - 1)).split("\n")
            tail = parts.pop()
            for line in parts:
                yield line + "\n"
        if tail:
            yield tail

    return csv.reader(lines())


def count(rows):
    header = next(rows)
    col = {c: i for i, c in enumerate(header)}
    counts, vacancies = Counter(), 0
    for row in rows:
        if is_variant(row[col["name"]], row[col["role"]]):
            vacancies += 1
            for a, b in combinations(parse_skills(row[col["key_skills"]]), 2):
                counts[(a, b)] += 1
    return counts, vacancies


def report(counts, vacancies):
    print(f"вакансий варианта: {vacancies}, разных пар: {len(counts)}, всего вхождений: {sum(counts.values())}")
    print("топ-10 пар:")
    top = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]
    for (a, b), n in top:
        print(f"  {n:>6}  {a} + {b}")
    return top


def peak():
    print(f"пик памяти процесса: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.1f} МБ")


def full():
    counts, vacancies = count(read_rows(RAW_KEY, {}))
    report(counts, vacancies)


def split():
    state = {}
    rows = read_rows(RAW_KEY, state)
    header = next(rows)
    col = {c: i for i, c in enumerate(header)}
    bufs = [io.StringIO() for _ in range(PARTS)]
    writers = [csv.writer(b, lineterminator="\n") for b in bufs]
    for w in writers:
        w.writerow(["name", "role", "key_skills"])
    sizes = [0] * PARTS
    for row in rows:
        p = min(PARTS - 1, state["chunk"] * PARTS // state["chunks"])
        writers[p].writerow([row[col["name"]], row[col["role"]], row[col["key_skills"]]])
        sizes[p] += 1
    for p in range(PARTS):
        s3.put_object(Bucket=BUCKET, Key=f"parts/part-{p}.csv", Body=bufs[p].getvalue().encode("utf-8"))
        print(f"parts/part-{p}.csv: строк {sizes[p]}")
    print(f"всего строк: {sum(sizes)}")


def map_part():
    p = int(os.environ["JOB_COMPLETION_INDEX"])
    counts, vacancies = count(read_rows(f"parts/part-{p}.csv", {}))
    out = {"vacancies": vacancies, "pairs": {f"{a}\t{b}": n for (a, b), n in counts.items()}}
    s3.put_object(Bucket=BUCKET, Key=f"counts/part-{p}.json", Body=json.dumps(out, ensure_ascii=False).encode("utf-8"))
    print(f"часть {p}: вакансий варианта {vacancies}, разных пар {len(counts)}")


def reduce_parts():
    counts, vacancies = Counter(), 0
    for p in range(PARTS):
        part = json.loads(s3.get_object(Bucket=BUCKET, Key=f"counts/part-{p}.json")["Body"].read())
        vacancies += part["vacancies"]
        for k, n in part["pairs"].items():
            counts[tuple(k.split("\t"))] += n
        print(f"прочитал counts/part-{p}.json")
    top = report(counts, vacancies)
    text = "\n".join(f"{n}\t{a}\t{b}" for (a, b), n in top) + "\n"
    s3.put_object(Bucket=BUCKET, Key="results/k8s_top10.tsv", Body=text.encode("utf-8"))


if __name__ == "__main__":
    {"full": full, "split": split, "map": map_part, "reduce": reduce_parts}[sys.argv[1]]()
    peak()
