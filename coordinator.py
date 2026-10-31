import codecs
import csv
import io
import json
import os
import re
from collections import Counter
from datetime import datetime
from itertools import combinations

import boto3
from botocore.config import Config
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

NAME = os.environ["NAME"]
RUN = os.environ["RUN"]
FENCING = os.environ.get("FENCING") == "1"
ZK = "zoo1:2181,zoo2:2181,zoo3:2181"
BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
CHUNK = 8 * 2**20

VARIANT_ROLE = "Программист, разработчик"
VARIANT_RE = re.compile(r"\bios\b|android|flutter|react[ -]?native|\bkmp\b|multiplatform|мобильн|\bmobile\b", re.ASCII)
EXCLUDE_RE = re.compile(r"\bqa\b|\baqa\b|тестиров|\btest", re.ASCII)

csv.field_size_limit(2**31 - 1)
os.makedirs("logs", exist_ok=True)
log_file = open(f"logs/{RUN}_{NAME}.log", "a", encoding="utf-8")


def log(msg):
    line = f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} [{NAME}] {msg}"
    print(line)
    log_file.write(line + "\n")
    log_file.flush()


s3 = boto3.client("s3", endpoint_url="http://rustfs:9000", aws_access_key_id="rustfsadmin",
                  aws_secret_access_key="rustfsadmin", region_name="us-east-1",
                  config=Config(s3={"addressing_style": "path"}))
zk = KazooClient(hosts=ZK, timeout=10)
base = f"/hw4/{RUN}"
ckpt_key = f"hw4/{RUN}/checkpoint.json"


def is_variant(name, role):
    name = name.lower()
    return role == VARIANT_ROLE and VARIANT_RE.search(name) is not None and EXCLUDE_RE.search(name) is None


def parse_skills(raw):
    return sorted({s.strip(" ") for s in raw.split(";")} - {""})


def read_header():
    head = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range="bytes=0-65535")["Body"].read()
    first = head.split(b"\n", 1)[0] + b"\n"
    header = next(csv.reader([first.decode("utf-8-sig")]))
    return header, len(first)


def rows_from(pos, size, state):
    decoder = codecs.getincrementaldecoder("utf-8")()

    def lines():
        tail = ""
        start = pos
        while start < size:
            end = min(start + CHUNK, size) - 1
            body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={start}-{end}")["Body"].read()
            state["chunks"] += 1
            parts = (tail + decoder.decode(body, final=end == size - 1)).split("\n")
            tail = parts.pop()
            for line in parts:
                state["pos"] += len(line.encode("utf-8")) + 1
                yield line + "\n"
            start = end + 1
        if tail:
            state["pos"] += len(tail.encode("utf-8"))
            yield tail

    return csv.reader(lines())


def save_checkpoint(ck):
    s3.put_object(Bucket=BUCKET, Key=ckpt_key, Body=json.dumps(ck, ensure_ascii=False).encode("utf-8"))


def load_checkpoint():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=ckpt_key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def lead():
    stat = zk.set(f"{base}/epoch", NAME.encode())
    epoch = stat.version
    log(f"стал лидером, эпоха {epoch}")

    header, header_len = read_header()
    col = {c: i for i, c in enumerate(header)}
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    ck = load_checkpoint()
    if ck:
        log(f"продолжаю с чекпоинта: байт {ck['pos']}, строк {ck['rows']}, записал {ck['by']}")
    else:
        ck = {"pos": header_len, "rows": 0, "vacancies": 0, "counts": {}, "by": NAME}
        log("чекпоинта нет, начинаю с начала")
    counts = Counter(ck["counts"])
    state = {"pos": ck["pos"], "chunks": 0}
    saved_chunks = 0
    for row in rows_from(ck["pos"], size, state):
        ck["rows"] += 1
        if is_variant(row[col["name"]], row[col["role"]]):
            ck["vacancies"] += 1
            for a, b in combinations(parse_skills(row[col["key_skills"]]), 2):
                counts[f"{a}\t{b}"] += 1
        if state["chunks"] != saved_chunks:
            saved_chunks = state["chunks"]
            ck.update(pos=state["pos"], counts=counts, by=NAME)
            save_checkpoint(ck)
            log(f"чекпоинт: {100 * state['pos'] / size:.0f} %, строк {ck['rows']}, вакансий варианта {ck['vacancies']}")
    ck.update(pos=state["pos"], counts=counts, by=NAME)
    save_checkpoint(ck)

    log(f"расчёт готов: строк {ck['rows']}, вакансий варианта {ck['vacancies']}, "
        f"разных пар {len(counts)}, всего вхождений {sum(counts.values())}")
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["skill1", "skill2", "count"])
    for pair, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        w.writerow(pair.split("\t") + [n])
    result_key = f"hw4/{RUN}/pairs_{NAME}.csv"
    s3.put_object(Bucket=BUCKET, Key=result_key, Body=buf.getvalue().encode("utf-8"))

    pointer = json.dumps({"result": result_key, "by": NAME, "epoch": epoch}).encode()
    if FENCING:
        tx = zk.transaction()
        tx.check(f"{base}/epoch", epoch)
        tx.set_data(f"{base}/result", pointer)
        errors = [r for r in tx.commit() if isinstance(r, Exception)]
        if errors:
            log(f"запись отклонена: {type(errors[0]).__name__}, моя эпоха {epoch} устарела")
        else:
            log(f"записал результат {result_key}")
    else:
        zk.set(f"{base}/result", pointer)
        log(f"записал результат {result_key}")
    data, stat = zk.get(f"{base}/result")
    log(f"указатель: {data.decode()}, версия {stat.version}")


zk.start()
zk.ensure_path(f"{base}/epoch")
try:
    zk.create(f"{base}/result", b"")
except NodeExistsError:
    pass
log(f"запущен, FENCING={int(FENCING)}, жду лидерства")
zk.Election(f"{base}/election", NAME).run(lead)
zk.stop()
