"""Test end-to-end della logica Spark su un DataFrame statico che imita il topic Kafka."""
import json
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from conftest import iceberg_jar

POSTS = [
    {"id": "1", "created_at": "2026-10-27T10:00:00.000Z", "language": "en", "source": "test",
     "content_html": "<p>I love this, it is <b>great</b>! &amp; good<br/>https://x.org/a</p>",
     "url": "u1", "author": "a", "hashtags": ["ai"], "replies_count": 1, "reblogs_count": 0, "favourites_count": 2},
    {"id": "2", "created_at": "2026-10-27T10:01:00Z", "language": "en", "source": "test",
     "content_html": "<p>terrible and broken</p>", "url": "u2", "author": "b", "hashtags": None,
     "replies_count": 0, "reblogs_count": 0, "favourites_count": 0},
    {"id": "", "created_at": None, "language": "en", "content_html": "<p></p>"},  # non valido
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    builder = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2"))
    jar = iceberg_jar()
    if jar:
        wh = str(tmp_path_factory.mktemp("warehouse"))
        builder = (builder.config("spark.jars", jar)
                   .config("spark.sql.extensions",
                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                   .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
                   .config("spark.sql.catalog.lake.type", "hadoop")
                   .config("spark.sql.catalog.lake.warehouse", wh))
    s = builder.getOrCreate()
    yield s
    s.stop()


def kafka_like(spark, posts):
    now = datetime(2026, 10, 27, 10, 5)
    rows = [(json.dumps(p).encode(), now) for p in posts]
    return spark.createDataFrame(rows, "value binary, timestamp timestamp")


def test_parse_clean_and_dedup(spark):
    from streaming_job import parse_posts
    df = parse_posts(kafka_like(spark, POSTS + [POSTS[0]]))   # post 1 duplicato
    rows = {r["id"]: r for r in df.collect()}
    assert sorted(rows) == ["", "1", "2"]
    assert rows["1"]["text"] == "I love this, it is great! & good"
    assert rows["1"]["is_valid"] and rows["2"]["is_valid"]
    assert not rows[""]["is_valid"]
    assert rows["2"]["hashtags"] == []


def test_enrich_runs_pandas_udfs(spark):
    from streaming_job import OUTPUT_COLUMNS, enrich, parse_posts
    out = enrich(parse_posts(kafka_like(spark, POSTS)).filter("is_valid")).select(*OUTPUT_COLUMNS)
    rows = {r["id"]: r for r in out.collect()}
    assert rows["1"]["sentiment"] == "positive"
    assert rows["2"]["sentiment"] == "negative"
    assert len(rows["1"]["embedding"]) == 384


@pytest.mark.skipif(iceberg_jar() is None, reason="metti iceberg-spark-runtime in tests/jars/")
def test_iceberg_merge_is_idempotent(spark):
    from streaming_job import ICEBERG_TABLE, OUTPUT_COLUMNS, create_iceberg_table, enrich, parse_posts, write_iceberg
    create_iceberg_table(spark)
    out = enrich(parse_posts(kafka_like(spark, POSTS)).filter("is_valid")).select(*OUTPUT_COLUMNS)
    write_iceberg(out)
    write_iceberg(out)   # stesso batch riprocessato (es. dopo un crash)
    assert spark.table(ICEBERG_TABLE).count() == 2
    assert spark.sql(f"SELECT count(*) FROM {ICEBERG_TABLE}.snapshots").first()[0] >= 1
    # schema evolution: una nuova colonna non rompe le scritture successive
    spark.sql(f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN reviewed BOOLEAN")
    write_iceberg(enrich(parse_posts(kafka_like(spark, [dict(POSTS[0], id="3")]))).select(*OUTPUT_COLUMNS))
    assert spark.table(ICEBERG_TABLE).count() == 3


@pytest.mark.skipif(iceberg_jar() is None, reason="metti iceberg-spark-runtime in tests/jars/")
def test_streaming_foreach_batch_end_to_end(spark, tmp_path, monkeypatch):
    """Stream reale (sorgente file al posto di Kafka) -> watermark dedup -> process_batch.
    Elasticsearch e' sostituito da finti client che registrano i documenti."""
    import elasticsearch
    import streaming_job as job
    from pyspark.sql import functions as F

    metrics = []

    class FakeES:
        def __init__(self, *a, **kw): pass
        def index(self, index, id, document): metrics.append(document)

    monkeypatch.setattr(elasticsearch, "Elasticsearch", FakeES)
    monkeypatch.setattr(job, "index_partition", lambda rows: None)
    monkeypatch.setattr(job, "ICEBERG_TABLE", "lake.tap.posts_stream")
    job.create_iceberg_table(spark)

    src = tmp_path / "in"
    src.mkdir()
    lines = [json.dumps(dict(POSTS[0], id=f"s{i % 3}")) for i in range(6)] + [json.dumps(POSTS[2])]
    (src / "part1.jsonl").write_text("\n".join(lines))

    raw = (spark.readStream.text(str(src))
           .select(F.col("value").cast("binary").alias("value"),
                   F.current_timestamp().alias("timestamp")))
    q = (job.parse_posts(raw).writeStream
         .foreachBatch(job.process_batch)
         .option("checkpointLocation", str(tmp_path / "chk"))
         .trigger(availableNow=True)
         .start())
    q.awaitTermination(120)

    spark.sql("REFRESH TABLE lake.tap.posts_stream")   # il foreachBatch usa una sessione clonata
    assert spark.table("lake.tap.posts_stream").count() == 3          # s0, s1, s2 senza duplicati
    assert metrics and metrics[0]["rows_invalid"] == 1
    assert metrics[0]["positive"] == 3
