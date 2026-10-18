import argparse

import common
from common import Profiler, connect, create_mobile

SQL = {
    1: """SELECT skill AS skill1, count(*) AS count FROM mobile_skill GROUP BY ALL""",
    2: """SELECT a.skill AS skill1, b.skill AS skill2, count(*) AS count
          FROM mobile_skill a JOIN mobile_skill b ON a.id = b.id AND a.skill < b.skill
          GROUP BY ALL""",
    3: """SELECT a.skill AS skill1, b.skill AS skill2, c.skill AS skill3, count(*) AS count
          FROM mobile_skill a
          JOIN mobile_skill b ON a.id = b.id AND a.skill < b.skill
          JOIN mobile_skill c ON b.id = c.id AND b.skill < c.skill
          GROUP BY ALL""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--format", choices=["csv", "parquet"], default="csv")
    ap.add_argument("--out", default="duckdb")
    args = ap.parse_args()

    with Profiler(f"duckdb {args.format}"):
        con = connect()
        create_mobile(con, args.format)
        n, n_sk = con.sql("SELECT count(*), count(*) FILTER (len(skills) > 0) FROM mobile").fetchone()
        print(f"DuckDB ({args.format}): вакансий варианта: {n}, из них с навыками: {n_sk}")
        print(f"{'k':>2} {'разных':>10} {'всего вхождений':>16}")
        for k in (1, 2, 3):
            cols = ", ".join(f"skill{i}" for i in range(1, k + 1))
            con.sql(f"CREATE OR REPLACE TABLE res{k} AS {SQL[k]}")
            key = f"{common.RESULTS}/{args.out}/k{k}.csv"
            con.sql(f"COPY (SELECT * FROM res{k} ORDER BY count DESC, {cols}) "
                    f"TO 's3://{common.BUCKET}/{key}' (HEADER, DELIMITER ',')")
            distinct, total = con.sql(f"SELECT count(*), sum(count) FROM res{k}").fetchone()
            print(f"{k:>2} {distinct:>10} {int(total or 0):>16}   -> s3://{common.BUCKET}/{key}")

        for k, title in ((1, "навыки"), (2, "пары"), (3, "тройки")):
            cols = ", ".join(f"skill{i}" for i in range(1, k + 1))
            print(f"\nТоп-10, {title}:")
            for row in con.sql(f"SELECT count, {cols} FROM res{k} ORDER BY count DESC, {cols} LIMIT 10").fetchall():
                print(f"  {row[0]:>6}  {' + '.join(row[1:])}")


if __name__ == "__main__":
    main()
