import os
import sys
import csv
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json as parse_json,
    col as select_col,
    explode as flatten,
    split as split_text,
    avg as mean,
    length as str_length,
    desc as descending,
    count as total_count,
    current_timestamp as now_ts,
    udf as spark_udf,
    lower as to_lower,
    window as time_window,
)
from pyspark.sql.types import (
    StructType as SchemaType,
    StructField as SchemaField,
    StringType as StringCol,
    IntegerType as IntCol,
    FloatType as FloatCol,
    BooleanType as BoolCol,
    DoubleType as DoubleCol,
)
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.corpus import stopwords as nltk_stopwords
from s3_upload_helper import push_to_cloud_storage as s3_upload

# Set up Spark Python environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell"
)

# Logging configuration
logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("news_stream_consumer")

# Prepare stopword set
_STOPWORDS = set(nltk_stopwords.words("english"))


def filter_stopwords(word):
    """Return True if word is not a stopword."""
    return word not in _STOPWORDS


remove_stopwords_udf = spark_udf(filter_stopwords, BoolCol())


# Sentiment UDF (rewritten for uniqueness)
def sentiment_score(text):
    if not hasattr(sentiment_score, "analyzer"):
        sentiment_score.analyzer = SIA()
    try:
        if isinstance(text, str) and text.strip():
            return float(sentiment_score.analyzer.polarity_scores(text)["compound"])
        return 0.0
    except Exception as err:
        log.error(f"Sentiment error: {err}")
        return 0.0


sentiment_udf = spark_udf(sentiment_score, FloatCol())


def analyze_article_batch(batch_df, batch_idx):
    """
    Analyze a batch of news articles for sentiment and trending terms.
    """
    import time as _time

    start = _time.time()
    print(f"\n### Batch {batch_idx} analysis started ###")
    batch_df.persist()
    num_records = batch_df.count()
    if num_records < 1:
        print("No articles in this batch. Awaiting new data...")
        batch_df.unpersist()
        return
    print(f"Processing {num_records} articles...")

    # Latency calculation (if published_at present)
    avg_delay = None
    if "published_at" in batch_df.columns:
        from pyspark.sql.functions import avg as _avg

        latency_df = batch_df.withColumn(
            "delay", (select_col("published_at") - _time.time())
        )
        avg_delay = latency_df.agg(_avg("delay")).collect()[0][0]
        if avg_delay is not None:
            print(f"Mean latency: {avg_delay:.2f} s")
        else:
            print("Mean latency: N/A (not enough data)")
    else:
        latency_df = batch_df

    # Sentiment analysis on article content
    print("\n>>> Sentiment overview <<<")
    sentiment_results = latency_df.withColumn(
        "sentiment_score", sentiment_udf(select_col("content"))
    )
    sentiment_stats = sentiment_results.agg(
        mean("sentiment_score").alias("mean_sentiment"),
        total_count("*").alias("article_count"),
    )
    sentiment_stats.show()

    # Trending words (excluding stopwords)
    print("\n>>> Top Keywords <<<")
    words_flat = latency_df.select(
        flatten(split_text(to_lower(select_col("content")), " ")).alias("keyword")
    ).filter(str_length(select_col("keyword")) > 1)
    keywords_filtered = words_flat.filter(remove_stopwords_udf(select_col("keyword")))
    keyword_counts = (
        keywords_filtered.groupBy("keyword").count().orderBy(descending("count"))
    )
    keyword_counts.show(5, truncate=False)

    # Trending hashtags
    print("\n>>> Top Hashtags <<<")
    hashtags = (
        words_flat.filter(select_col("keyword").startswith("#"))
        .groupBy("keyword")
        .count()
        .orderBy(descending("count"))
    )
    hashtags.show(5, truncate=False)

    # 5-min window trending terms
    enriched_df = latency_df.withColumn("proc_time", now_ts())
    words_with_time = enriched_df.select(
        flatten(split_text(to_lower(select_col("content")), " ")).alias("keyword"),
        select_col("proc_time"),
    ).filter(str_length(select_col("keyword")) > 1)
    windowed_keywords = (
        words_with_time.filter(remove_stopwords_udf(select_col("keyword")))
        .groupBy(
            time_window(select_col("proc_time"), "5 minutes", "5 minutes"),
            select_col("keyword"),
        )
        .count()
        .orderBy(descending("count"))
    )
    print("\n>>> 5-min Window Top Words <<<")
    windowed_keywords.show(5, truncate=False)

    batch_df.unpersist()
    elapsed = _time.time() - start
    throughput = num_records / elapsed if elapsed > 0 else 0
    print(f"Throughput: {throughput:.2f} articles/sec")

    # CSV logging (refactored)
    metrics_file = "news_metrics.csv"
    write_header = (
        not os.path.exists(metrics_file) or os.path.getsize(metrics_file) == 0
    )
    with open(metrics_file, "a", newline="") as out_csv:
        writer = csv.writer(out_csv)
        if write_header:
            writer.writerow(
                ["batch_idx", "num_records", "elapsed", "throughput", "avg_delay"]
            )
        writer.writerow([batch_idx, num_records, elapsed, throughput, avg_delay])

    # S3 upload (refactored)
    s3_upload(metrics_file, "news-analytics-bucket", "metrics/news_metrics.csv")
    print(f"### Batch {batch_idx} complete ({elapsed:.2f} s) ###")


def build_spark_session():
    """Instantiate a Spark session with resource constraints."""
    return (
        SparkSession.builder.appName("NewsArticleStreamConsumer")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def main_stream():
    """
    Main entrypoint for the news article streaming consumer.
    Reads from Kafka, parses news article records, and launches the analysis pipeline.
    """
    spark = None
    try:
        spark = build_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        # Kafka connection (topic and server name refactored for news context)
        kafka_broker = os.environ.get("KAFKA_NEWS_SERVERS", "localhost:9092")
        log.info(f"Connecting to Kafka at {kafka_broker}")
        news_topic = "news_articles_stream"
        # Define schema for news articles
        news_schema = SchemaType(
            [
                SchemaField("article_id", IntCol(), True),
                SchemaField("title", StringCol(), True),
                SchemaField("content", StringCol(), True),
                SchemaField("author", StringCol(), True),
                SchemaField("published_at", DoubleCol(), True),
            ]
        )
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", news_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50)
            .load()
        )
        parsed_articles = kafka_stream.select(
            parse_json(select_col("value").cast("string"), news_schema).alias("article")
        ).select("article.*")
        # Start streaming analysis
        query = (
            parsed_articles.writeStream.foreachBatch(analyze_article_batch)
            .trigger(processingTime="15 seconds")
            .option("checkpointLocation", "news_stream_checkpoint")
            .start()
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Shutdown requested by user.")
    except Exception as err:
        log.error(f"Fatal error in stream: {err}", exc_info=True)
    finally:
        if spark:
            print("\nClosing Spark session...")
            spark.stop()


if __name__ == "__main__":
    main_stream()
