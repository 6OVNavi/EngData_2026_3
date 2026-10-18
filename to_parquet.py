import common
from common import Profiler, connect, source

COLS = ["area", "name", "role", "salary_from", "salary_to", "currency", "key_skills"]


def main():
    with Profiler("to_parquet"):
        con = connect()
        con.sql(f"COPY (SELECT {', '.join(COLS)} FROM {source('csv')}) "
                f"TO 's3://{common.BUCKET}/{common.PARQUET_KEY}' (FORMAT parquet, COMPRESSION zstd)")
        n = con.sql(f"SELECT count(*) FROM {source('parquet')}").fetchone()[0]
        print(f"Готово: s3://{common.BUCKET}/{common.PARQUET_KEY}, строк: {n}")


if __name__ == "__main__":
    main()
