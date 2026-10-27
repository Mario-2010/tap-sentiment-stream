"""Demo Lakehouse su Apache Iceberg: analytics SQL, snapshot, Time Travel e Schema Evolution.

Uso (con la pipeline attiva):
    docker compose exec spark spark-submit /opt/app/iceberg_demo.py
    docker compose exec spark spark-submit /opt/app/iceberg_demo.py --evolve
"""
import sys

from streaming_job import ICEBERG_TABLE, build_spark


def title(text: str) -> None:
    print(f"\n{'=' * 70}\n{text}\n{'=' * 70}")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    title("1. Analytics SQL sul Lakehouse: sentiment per ora")
    spark.sql(f"""
        SELECT date_trunc('hour', created_at) AS ora,
               count(*) AS post,
               round(avg(sentiment_value), 3) AS sentiment_medio,
               sum(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positivi,
               sum(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negativi
        FROM {ICEBERG_TABLE} GROUP BY 1 ORDER BY 1 DESC LIMIT 12
    """).show(truncate=False)

    title("2. Hashtag piu' usati e loro sentiment medio")
    spark.sql(f"""
        SELECT tag, count(*) AS post, round(avg(sentiment_value), 3) AS sentiment_medio
        FROM {ICEBERG_TABLE} LATERAL VIEW explode(hashtags) t AS tag
        GROUP BY tag ORDER BY post DESC LIMIT 10
    """).show(truncate=False)

    title("3. Snapshot della tabella (ogni micro-batch = un commit atomico)")
    snapshots = spark.sql(f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records'] AS aggiunti, summary['total-records'] AS totali
        FROM {ICEBERG_TABLE}.snapshots ORDER BY committed_at
    """)
    snapshots.show(10, truncate=False)

    rows = snapshots.collect()
    if len(rows) >= 2:
        first = rows[0]
        title(f"4. Time Travel: la tabella com'era al primo snapshot ({first['committed_at']})")
        old = spark.sql(f"SELECT count(*) AS n FROM {ICEBERG_TABLE} VERSION AS OF {first['snapshot_id']}").first()["n"]
        now = spark.sql(f"SELECT count(*) AS n FROM {ICEBERG_TABLE}").first()["n"]
        print(f"Righe al primo snapshot: {old}  |  righe oggi: {now}")
        ts = rows[1]["committed_at"].strftime("%Y-%m-%d %H:%M:%S.%f")
        n = spark.sql(f"SELECT count(*) AS n FROM {ICEBERG_TABLE} TIMESTAMP AS OF '{ts}'").first()["n"]
        print(f"Righe all'istante {ts} (secondo snapshot): {n}")
    else:
        print("Servono almeno due snapshot: attendi qualche micro-batch e riprova.")

    title("5. File di dati e partizioni (metadata tables)")
    spark.sql(f"SELECT partition, record_count, file_count FROM {ICEBERG_TABLE}.partitions").show(truncate=False)

    if "--evolve" in sys.argv:
        title("6. Schema Evolution: aggiungo una colonna senza riscrivere i dati")
        if "reviewed" not in spark.table(ICEBERG_TABLE).columns:
            spark.sql(f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN reviewed BOOLEAN")
        spark.sql(f"DESCRIBE TABLE {ICEBERG_TABLE}").show(30, truncate=False)
        print("Le righe vecchie hanno reviewed = NULL; lo streaming continua a scrivere senza errori.")
        spark.sql(f"SELECT id, sentiment, reviewed FROM {ICEBERG_TABLE} LIMIT 5").show(truncate=False)


if __name__ == "__main__":
    main()
