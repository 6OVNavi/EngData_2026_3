import common
from common import Profiler, connect, create_mobile, show


def main():
    with Profiler("salary"):
        con = connect()
        create_mobile(con)
        con.sql("CREATE TABLE ss AS SELECT * FROM mobile_skill WHERE salary IS NOT NULL")
        con.sql("CREATE TABLE f1 AS SELECT skill, count(*) AS n FROM mobile_skill GROUP BY ALL")
        con.sql("""CREATE TABLE f2 AS SELECT a.skill AS s1, b.skill AS s2, count(*) AS n
                   FROM mobile_skill a JOIN mobile_skill b ON a.id = b.id AND a.skill < b.skill
                   GROUP BY ALL""")
        con.sql("CREATE TABLE m1 AS SELECT skill, count(*) AS n, median(salary) AS med FROM ss GROUP BY ALL")
        con.sql("""CREATE TABLE m2 AS SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(a.salary) AS med
                   FROM ss a JOIN ss b ON a.id = b.id AND a.skill < b.skill GROUP BY ALL""")
        con.sql("""CREATE TABLE m3 AS SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n, median(a.salary) AS med
                   FROM ss a JOIN ss b ON a.id = b.id AND a.skill < b.skill
                             JOIN ss c ON b.id = c.id AND b.skill < c.skill
                   GROUP BY ALL""")

        n_all, n_sal, med_all = con.sql("SELECT count(*), count(salary), median(salary) FROM mobile").fetchone()
        print(f"Вакансий варианта: {n_all}, с зарплатой в рублях: {n_sal}, медиана по варианту: {med_all:,.0f} ₽".replace(",", " "))

        con.sql(f"""CREATE TABLE skill_tab AS
            SELECT skill AS навык, n AS вакансий_с_зп, med AS медиана, med - {med_all} AS разница_с_медианой_варианта
            FROM m1 WHERE n >= {common.MIN_SKILL}""")

        con.sql(f"""CREATE TABLE pair_tab AS
            SELECT base AS базовый_навык, f_base AS частота_базового, mb.n AS вакансий_с_зп_базовый, mb.med AS медиана_базового,
                   added AS добавленный_навык, p.n AS вакансий_с_зп_пара, p.med AS медиана_пары, p.med - mb.med AS прибавка
            FROM (
                SELECT m2.*, 
                       CASE WHEN a.n >= b.n THEN s1 ELSE s2 END AS base,
                       CASE WHEN a.n >= b.n THEN s2 ELSE s1 END AS added,
                       greatest(a.n, b.n) AS f_base
                FROM m2 JOIN f1 a ON a.skill = m2.s1 JOIN f1 b ON b.skill = m2.s2
                WHERE m2.n >= {common.MIN_PAIR}
            ) p JOIN m1 mb ON mb.skill = p.base""")

        con.sql(f"""CREATE TABLE triple_tab AS
            WITH t AS (SELECT * FROM m3 WHERE n >= {common.MIN_TRIPLE}),
            cand AS (
                SELECT t.*, x.p1, x.p2, x.add_skill, f2.n AS f_pair
                FROM t, LATERAL (VALUES (t.s1, t.s2, t.s3), (t.s1, t.s3, t.s2), (t.s2, t.s3, t.s1)) x(p1, p2, add_skill)
                JOIN f2 ON f2.s1 = x.p1 AND f2.s2 = x.p2
                QUALIFY row_number() OVER (PARTITION BY t.s1, t.s2, t.s3 ORDER BY f2.n DESC, x.p1, x.p2) = 1
            )
            SELECT cand.p1 || ' + ' || cand.p2 AS базовая_пара, cand.f_pair AS частота_пары, mb.n AS вакансий_с_зп_пара,
                   mb.med AS медиана_пары, cand.add_skill AS добавленный_навык, cand.n AS вакансий_с_зп_тройка,
                   cand.med AS медиана_тройки, cand.med - mb.med AS прибавка
            FROM cand JOIN m2 mb ON mb.s1 = cand.p1 AND mb.s2 = cand.p2""")

        for tab, what, n in (("skill_tab", "навыки", 10), ("pair_tab", "пары", 10), ("triple_tab", "тройки", 10)):
            col = "разница_с_медианой_варианта" if tab == "skill_tab" else "прибавка"
            total = con.sql(f"SELECT count(*) FROM {tab}").fetchone()[0]
            show(con, f"SELECT * FROM {tab} WHERE {col} > 0 ORDER BY {col} DESC, 1, 2 LIMIT {n}",
                 f"{what.upper()}: сильнее всего ПОВЫШАЮТ медиану (из {total} с достаточной выборкой):")
            show(con, f"SELECT * FROM {tab} WHERE {col} < 0 ORDER BY {col} ASC, 1, 2 LIMIT {n}",
                 f"{what.upper()}: сильнее всего ПОНИЖАЮТ медиану:")


if __name__ == "__main__":
    main()
