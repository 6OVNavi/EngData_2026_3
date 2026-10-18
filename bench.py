import argparse
import json
import os
import statistics
import subprocess
import sys

RUNS = {
    "MapReduce (4 процесса, combiner)": ["mr_skills.py"],
    "DuckDB, CSV из S3": ["duck_skills.py"],
}
BONUS = {
    "ДО: MapReduce, 1 процесс, без combiner": ["mr_skills.py", "--workers", "1", "--no-combiner", "--out", "mapreduce_base"],
    "ПОСЛЕ: DuckDB, Parquet из S3": ["duck_skills.py", "--format", "parquet", "--out", "duckdb_parquet"],
}


def run_once(cmd):
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    p = subprocess.run([sys.executable] + cmd, capture_output=True, text=True, encoding="utf-8", env=env)
    if p.returncode != 0:
        print(p.stdout, p.stderr)
        raise SystemExit(f"Упал запуск: {' '.join(cmd)}")
    line = [l for l in p.stdout.splitlines() if l.startswith("PROFILE ")][-1]
    return json.loads(line[len("PROFILE "):])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=3)
    ap.add_argument("--bonus", action="store_true")
    args = ap.parse_args()

    runs = dict(RUNS, **BONUS) if args.bonus else RUNS
    table = []
    for title, cmd in runs.items():
        for i in range(1, args.n + 1):
            r = run_once(cmd)
            print(f"{title} | запуск {i}: {r['seconds']} с, пик {r['peak_mb']} МБ", flush=True)
            table.append({"вариант": title, "запуск": i, "секунд": r["seconds"], "пик_МБ": r["peak_mb"]})

    print(f"\n{'вариант':<42} {'время, с (среднее)':>20} {'мин–макс':>14} {'пик памяти, МБ':>16} {'мин–макс':>16}")
    for title in runs:
        t = [x["секунд"] for x in table if x["вариант"] == title]
        m = [x["пик_МБ"] for x in table if x["вариант"] == title]
        print(f"{title:<42} {statistics.mean(t):>20.2f} {f'{min(t)}–{max(t)}':>14} "
              f"{statistics.mean(m):>16.0f} {f'{min(m)}–{max(m)}':>16}")


if __name__ == "__main__":
    main()
