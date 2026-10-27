"""Spark Structured Streaming: Kafka -> pulizia -> AI inference (ONNX) -> Iceberg + Elasticsearch.

Flusso di ogni micro-batch:
  1. legge i post JSON dal topic Kafka e li deserializza con uno schema esplicito;
  2. pulisce il testo (HTML, entita', link) e marca i record non validi;
  3. elimina i duplicati (il polling di Mastodon restituisce piu' volte lo stesso post);
  4. in foreachBatch: calcola sentiment ed embedding con Pandas UDF che eseguono
     modelli ONNX, poi scrive su Iceberg (MERGE, idempotente) ed Elasticsearch
     (id del documento = id del post, idempotente) e registra le metriche del batch.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon-posts")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "mastodon-posts")
ES_METRICS_INDEX = os.getenv("ES_METRICS_INDEX", "pipeline-metrics")
WAREHOUSE = os.getenv("WAREHOUSE", "/warehouse")
CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoints/posts")
ICEBERG_TABLE = "lake.tap.posts"
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "200")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# Contratto del dato in ingresso (prodotto da Logstash)
POST_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("content_html", T.StringType()),
    T.StructField("language", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("hashtags", T.ArrayType(T.StringType())),
    T.StructField("source", T.StringType()),
    T.StructField("replies_count", T.LongType()),
    T.StructField("reblogs_count", T.LongType()),
    T.StructField("favourites_count", T.LongType()),
])

SENTIMENT_TYPE = T.StructType([
    T.StructField("label", T.StringType()),
    T.StructField("score", T.FloatType()),
    T.StructField("value", T.FloatType()),
])

OUTPUT_COLUMNS = [
    "id", "source", "author", "url", "language", "text", "hashtags", "created_at",
    "processed_at", "replies_count", "reblogs_count", "favourites_count",
    "sentiment", "sentiment_score", "sentiment_value", "embedding",
]


# ------------------------------------------------------------------ AI inference
# Pandas UDF "iterator of Series": il modello viene caricato UNA volta per task
# (non per riga, ne' per batch Arrow) e poi applicato a blocchi vettorizzati.

@F.pandas_udf(SENTIMENT_TYPE)
def sentiment_udf(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    from onnx_models import SentimentModel
    model = SentimentModel()
    for texts in batches:
        preds = model.predict(texts.fillna("").tolist())
        yield pd.DataFrame(preds, columns=["label", "score", "value"])


@F.pandas_udf(T.ArrayType(T.FloatType()))
def embedding_udf(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    from onnx_models import EmbeddingModel
    model = EmbeddingModel()
    for texts in batches:
        vectors = model.encode(texts.fillna("").tolist())
        yield pd.Series(list(vectors))


# ------------------------------------------------------------------ trasformazioni

def clean_text(col):
    """HTML di Mastodon -> testo semplice."""
    c = F.regexp_replace(col, r"(?i)<br\s*/?>|</p>", " ")
    c = F.regexp_replace(c, r"<[^>]+>", "")
    for entity, char in [("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"),
                         ("&lt;", "<"), ("&gt;", ">"), ("&nbsp;", " ")]:
        c = F.regexp_replace(c, entity, char)
    c = F.regexp_replace(c, r"https?://\S+", "")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def parse_posts(raw: DataFrame) -> DataFrame:
    posts = (raw
             .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("p"),
                     F.col("timestamp").alias("kafka_ts"))
             .select("p.*", "kafka_ts")
             .withColumn("text", clean_text(F.col("content_html")))
             .withColumn("created_at", F.col("created_at").cast("timestamp"))
             .withColumn("hashtags", F.coalesce(F.col("hashtags"), F.array().cast("array<string>")))
             .withColumn("is_valid", F.col("id").isNotNull()
                         & F.col("created_at").isNotNull()
                         & (F.length("text") >= 3)))
    if not posts.isStreaming:          # usato nei test su DataFrame statici
        return posts.dropDuplicates(["id"])
    # Deduplica sul tempo di ingestione (kafka_ts): un post rivisto entro 15 minuti e' scartato
    return (posts
            .withWatermark("kafka_ts", "15 minutes")
            .dropDuplicatesWithinWatermark(["id"]))


def enrich(posts: DataFrame) -> DataFrame:
    return (posts
            .withColumn("s", sentiment_udf("text"))
            .withColumn("embedding", embedding_udf("text"))
            .withColumn("sentiment", F.col("s.label"))
            .withColumn("sentiment_score", F.col("s.score"))
            .withColumn("sentiment_value", F.col("s.value"))
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("replies_count", F.col("replies_count").cast("int"))
            .withColumn("reblogs_count", F.col("reblogs_count").cast("int"))
            .withColumn("favourites_count", F.col("favourites_count").cast("int"))
            .drop("s"))


# ------------------------------------------------------------------ sink

def create_iceberg_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.tap")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            id STRING, source STRING, author STRING, url STRING, language STRING,
            text STRING, hashtags ARRAY<STRING>, created_at TIMESTAMP, processed_at TIMESTAMP,
            replies_count INT, reblogs_count INT, favourites_count INT,
            sentiment STRING, sentiment_score FLOAT, sentiment_value FLOAT,
            embedding ARRAY<FLOAT>
        ) USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES ('format-version' = '2')
    """)


def write_iceberg(df: DataFrame) -> None:
    # MERGE: se un batch viene rieseguito dopo un crash i post gia' presenti non si duplicano.
    # Colonne elencate esplicitamente: la tabella puo' evolvere (nuove colonne) senza rompere il job.
    df.createOrReplaceTempView("updates")
    cols = ", ".join(OUTPUT_COLUMNS)
    values = ", ".join(f"s.{c}" for c in OUTPUT_COLUMNS)
    df.sparkSession.sql(f"""
        MERGE INTO {ICEBERG_TABLE} t USING updates s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({values})
    """)


def _iso(ts):
    return ts.replace(tzinfo=timezone.utc).isoformat() if ts else None


def index_partition(rows) -> None:
    """Eseguita sugli executor: ogni partizione scrive su Elasticsearch con la Bulk API."""
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch(ES_URL, request_timeout=60)

    def actions():
        for r in rows:
            doc = r.asDict()
            doc["created_at"] = _iso(doc["created_at"])
            doc["processed_at"] = _iso(doc["processed_at"])
            doc["embedding"] = [float(x) for x in doc["embedding"]]
            yield {"_index": ES_INDEX, "_id": doc["id"], "_source": doc}

    helpers.bulk(es, actions(), chunk_size=200)


def process_batch(batch: DataFrame, batch_id: int) -> None:
    start = time.time()
    batch = batch.persist()
    rows_in = batch.count()
    if rows_in == 0:
        batch.unpersist()
        return

    # L'inferenza e' costosa: il risultato viene messo in cache e riusato dai due sink
    enriched = enrich(batch.filter("is_valid")).persist()
    output = enriched.select(*OUTPUT_COLUMNS)

    write_iceberg(output)
    output.foreachPartition(index_partition)

    stats = (enriched.agg(
        F.count("*").alias("valid"),
        F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
        F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral"),
        F.avg(F.col("processed_at").cast("double") - F.col("kafka_ts").cast("double")).alias("latency"),
    ).first())
    metrics = {
        "batch_id": batch_id,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "rows_in": rows_in,
        "rows_valid": int(stats["valid"]),
        "rows_invalid": rows_in - int(stats["valid"]),
        "positive": int(stats["positive"] or 0),
        "negative": int(stats["negative"] or 0),
        "neutral": int(stats["neutral"] or 0),
        "avg_latency_s": float(stats["latency"] or 0.0),
        "duration_s": time.time() - start,
    }
    from elasticsearch import Elasticsearch
    Elasticsearch(ES_URL).index(index=ES_METRICS_INDEX, id=str(batch_id), document=metrics)
    print(f"[batch {batch_id}] {metrics}", flush=True)

    enriched.unpersist()
    batch.unpersist()


# ------------------------------------------------------------------ main

def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("tap-sentiment-stream")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "64")
            .getOrCreate())


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    create_iceberg_table(spark)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", MAX_OFFSETS)   # backpressure
           .option("failOnDataLoss", "false")
           .load())

    query = (parse_posts(raw).writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)     # offset + stato: ripartenza dopo un crash
             .trigger(processingTime=TRIGGER)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
