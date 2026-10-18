import argparse
import codecs
import csv
import io
import zlib
from itertools import combinations
from multiprocessing import Pool

import common
from common import Profiler, is_variant, parse_skills, s3_client

csv.field_size_limit(2**31 - 1)


def partition(key, n_reducers):
    return zlib.crc32(repr(key).encode("utf-8")) % n_reducers


def map_vacancy(row):
    name, role, key_skills = row
    if not is_variant(name, role):
        return
    skills = parse_skills(key_skills)
    yield len(skills), 1
    for k in (1, 2, 3):
        for combo in combinations(skills, k):
            yield combo, 1


def run_map(args):
    task_id, attempt, rows, n_reducers, use_combiner, fail_task = args
    if task_id == fail_task and attempt == 1:
        raise RuntimeError("имитация отказа маппера")
    parts = [{} for _ in range(n_reducers)]
    emitted = 0
    for row in rows:
        for key, value in map_vacancy(row):
            emitted += 1
            p = parts[partition(key, n_reducers)]
            if use_combiner:
                p[key] = p.get(key, 0) + value
            else:
                p.setdefault(key, []).append(value)
    return parts, emitted


def run_reduce(pieces):
    out = {}
    for piece in pieces:
        for key, value in piece.items():
            out[key] = out.get(key, 0) + (value if isinstance(value, int) else sum(value))
    return out


def read_chunks(client, chunk_mb, stats):
    size = client.head_object(Bucket=common.BUCKET, Key=common.RAW_KEY)["ContentLength"]
    step = chunk_mb * 2**20
    n_chunks = (size + step - 1) // step
    print(f"Объект s3://{common.BUCKET}/{common.RAW_KEY}: {size} байт, читаем кусками по {chunk_mb} МБ, кусков: {n_chunks}")
    decoder = codecs.getincrementaldecoder("utf-8-sig")()
    now = {"chunk": 0}

    def lines():
        tail = ""
        for i in range(n_chunks):
            start = i * step
            end = min(start + step, size) - 1
            body = client.get_object(Bucket=common.BUCKET, Key=common.RAW_KEY, Range=f"bytes={start}-{end}")["Body"].read()
            now["chunk"] = i
            parts = (tail + decoder.decode(body, final=(i == n_chunks - 1))).split("\n")
            tail = parts.pop()
            for line in parts:
                yield line + "\n"
        if tail:
            yield tail

    reader = csv.reader(lines())
    header = next(reader)
    name, role, skills = header.index("name"), header.index("role"), header.index("key_skills")
    batch, batch_chunk = [], 0
    for row in reader:
        if now["chunk"] != batch_chunk and batch:
            yield batch_chunk, batch
            batch, batch_chunk = [], now["chunk"]
        batch.append((row[name], row[role], row[skills]))
        stats["rows"] += 1
    if batch:
        yield batch_chunk, batch


def run_job(tasks, workers, use_combiner, fail_task):
    map_out, emitted, sent, restarts = [], 0, 0, 0

    def collect(task_id, rows, res):
        nonlocal emitted, sent, restarts
        attempt = 1
        while True:
            try:
                parts, n = res.get()
                break
            except RuntimeError as e:
                print(f"map-задача {task_id}, попытка {attempt}: ОШИБКА {e!r}, перезапускаю")
                restarts += 1
                attempt += 1
                res = pool.apply_async(run_map, ((task_id, attempt, rows, common.REDUCERS, use_combiner, fail_task),))
        if attempt > 1:
            print(f"map-задача {task_id} выполнена с попытки {attempt}")
        map_out.append(parts)
        emitted += n
        sent += sum(len(p) for p in parts) if use_combiner else n

    with Pool(workers) as pool:
        submitted = [(task_id, rows, pool.apply_async(run_map, ((task_id, 1, rows, common.REDUCERS, use_combiner, fail_task),)))
                     for task_id, rows in tasks]
        for item in submitted:
            collect(*item)
        result = {}
        for part in pool.map(run_reduce, [[parts[r] for parts in map_out] for r in range(common.REDUCERS)]):
            result.update(part)

    print(f"map-задач: {len(map_out)}, пар (ключ, значение) из map: {emitted}, "
          f"ушло в shuffle: {sent}, перезапусков: {restarts}")
    return result


def save(client, rows, k, folder):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([f"skill{i}" for i in range(1, k + 1)] + ["count"])
    for combo, n in rows:
        w.writerow(list(combo) + [n])
    key = f"{common.RESULTS}/{folder}/k{k}.csv"
    client.put_object(Bucket=common.BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))
    return key


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=common.WORKERS)
    ap.add_argument("--no-combiner", action="store_true")
    ap.add_argument("--fail-map", type=int)
    ap.add_argument("--out", default="mapreduce")
    args = ap.parse_args()
    use_combiner = not args.no_combiner
    print(f"MapReduce: workers={args.workers}, reducers={common.REDUCERS}, chunk={common.CHUNK_MB} МБ, "
          f"combiner={'да' if use_combiner else 'нет'}")

    with Profiler(f"mapreduce w{args.workers} c{int(use_combiner)}"):
        client = s3_client()
        read = {"rows": 0}
        result = run_job(read_chunks(client, common.CHUNK_MB, read), args.workers, use_combiner, args.fail_map)
        hist = {n: c for n, c in result.items() if isinstance(n, int)}
        print(f"Прочитано строк CSV: {read['rows']}")
        print(f"Вакансий варианта: {sum(hist.values())}, из них с навыками: {sum(c for n, c in hist.items() if n > 0)}")

        print("\nРост числа сочетаний: вакансия с n навыками даёт C(n,1)=n навыков, C(n,2) пар и C(n,3) троек")
        print(f"{'k':>2} {'разных':>10} {'всего вхождений':>16}")
        top = {}
        for k in (1, 2, 3):
            rows = sorted(((c, n) for c, n in result.items() if isinstance(c, tuple) and len(c) == k),
                          key=lambda x: (-x[1], x[0]))
            top[k] = rows[:10]
            key = save(client, rows, k, args.out)
            print(f"{k:>2} {len(rows):>10} {sum(n for _, n in rows):>16}   -> s3://{common.BUCKET}/{key}")
        with_skills = {n: c for n, c in hist.items() if n > 0}
        print(f"Навыков в вакансии: в среднем {sum(n * c for n, c in with_skills.items()) / sum(with_skills.values()):.1f}, "
              f"максимум {max(with_skills)}")

        for k, title in ((1, "навыки"), (2, "пары"), (3, "тройки")):
            print(f"\nТоп-10, {title}:")
            for combo, n in top[k]:
                print(f"  {n:>6}  {' + '.join(combo)}")


if __name__ == "__main__":
    main()
