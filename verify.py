import argparse

import common
from common import connect


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mr", default="mapreduce")
    ap.add_argument("--duck", default="duckdb")
    args = ap.parse_args()
    con = connect()
    ok = True
    for k in (1, 2, 3):
        cols = ", ".join(f"skill{i}" for i in range(1, k + 1))
        mr = f"read_csv('s3://{common.BUCKET}/{common.RESULTS}/{args.mr}/k{k}.csv', header=true, all_varchar=true)"
        dd = f"read_csv('s3://{common.BUCKET}/{common.RESULTS}/{args.duck}/k{k}.csv', header=true, all_varchar=true)"
        n_mr = con.sql(f"SELECT count(*), sum(count::BIGINT) FROM {mr}").fetchone()
        n_dd = con.sql(f"SELECT count(*), sum(count::BIGINT) FROM {dd}").fetchone()
        only_mr = con.sql(f"SELECT count(*) FROM (SELECT {cols}, count FROM {mr} EXCEPT SELECT {cols}, count FROM {dd})").fetchone()[0]
        only_dd = con.sql(f"SELECT count(*) FROM (SELECT {cols}, count FROM {dd} EXCEPT SELECT {cols}, count FROM {mr})").fetchone()[0]
        same = only_mr == 0 and only_dd == 0 and n_mr == n_dd
        ok &= same
        print(f"k={k}: {args.mr} vs {args.duck}: MapReduce строк {n_mr[0]}, сумма {n_mr[1]} | DuckDB строк {n_dd[0]}, сумма {n_dd[1]} | "
              f"есть только в MR: {only_mr}, только в DuckDB: {only_dd} -> {'СОВПАЛО' if same else 'НЕ СОВПАЛО'}")
    print("\nИТОГ:", "результаты полностью совпадают" if ok else "ЕСТЬ РАСХОЖДЕНИЯ")


if __name__ == "__main__":
    main()
