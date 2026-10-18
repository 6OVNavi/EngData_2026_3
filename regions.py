import argparse

from common import Profiler, connect, create_mobile, show


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--regions", nargs=3)
    args = ap.parse_args()

    with Profiler("regions"):
        con = connect()
        create_mobile(con)
        if args.regions:
            regions = args.regions
        else:
            regions = [r[0] for r in con.sql(
                "SELECT area FROM mobile GROUP BY area ORDER BY count(*) DESC, area LIMIT 3").fetchall()]
        print("Регионы:", ", ".join(regions))
        con.execute("CREATE TABLE r AS SELECT * FROM mobile WHERE list_contains(?, area)", [regions])
        con.sql("CREATE TABLE rs AS SELECT id, area, unnest(skills) AS skill, salary FROM r")

        show(con, """
            SELECT area AS регион, count(*) AS вакансий, count(salary) AS с_зп, median(salary) AS зп_медиана
            FROM r GROUP BY area ORDER BY вакансий DESC""", "Таблица 1. Зарплаты по регионам")

        con.sql("CREATE TABLE base AS SELECT area, count(*) AS n FROM r WHERE len(skills) > 0 GROUP BY area")
        con.sql("""
            CREATE TABLE combos AS
            SELECT area, 1 AS k, skill AS combo, count(*) AS cnt, median(salary) AS med FROM rs GROUP BY ALL
            UNION ALL
            SELECT a.area, 2, a.skill || ' + ' || b.skill, count(*), median(a.salary)
            FROM rs a JOIN rs b ON a.id = b.id AND a.skill < b.skill GROUP BY ALL
            UNION ALL
            SELECT a.area, 3, a.skill || ' + ' || b.skill || ' + ' || c.skill, count(*), median(a.salary)
            FROM rs a JOIN rs b ON a.id = b.id AND a.skill < b.skill
                      JOIN rs c ON b.id = c.id AND b.skill < c.skill GROUP BY ALL""")
        show(con, """
            SELECT c.area AS регион, CASE k WHEN 1 THEN 'навык' WHEN 2 THEN 'пара' ELSE 'тройка' END AS что,
                   combo AS сочетание, cnt AS вакансий, round(100.0 * cnt / b.n, 1) AS доля_проц, med AS медиана_зп
            FROM combos c JOIN base b USING (area)
            QUALIFY row_number() OVER (PARTITION BY c.area, k ORDER BY cnt DESC, combo) <= CASE k WHEN 1 THEN 5 ELSE 3 END
            ORDER BY b.n DESC, c.area, k, cnt DESC, combo""",
             "Таблица 2. Топ-5 навыков, топ-3 пары и топ-3 тройки в каждом регионе (доля от вакансий с навыками)")


if __name__ == "__main__":
    main()
