import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import kagglehub
import logging
import os


def initialize_article_producer():
    """
    Establish a Kafka producer for streaming news articles.
    Retries connection multiple times for robustness.
    """
    max_attempts = 12
    kafka_host = os.environ.get("KAFKA_NEWS_SERVERS", "localhost:9092")
    log = logging.getLogger("news_article_producer")
    log.info(f"Attempting Kafka connection at {kafka_host}")
    for attempt in range(max_attempts):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_host,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("Kafka producer initialized.")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"Kafka unavailable, retrying in 5s... ({attempt+1}/{max_attempts})"
            )
            time.sleep(5)
    log.error("Kafka connection failed after retries.")
    return None


def publish_news_stream(producer, topic, csv_path):
    """
    Stream news articles from a CSV file to a Kafka topic.
    Each row must contain: article_id, title, content, author, published_at.
    """
    log = logging.getLogger("news_article_producer")
    try:
        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
        except UnicodeDecodeError:
            log.warning("UTF-8 decode error, trying latin1 encoding")
            df = pd.read_csv(csv_path, encoding="latin1")
        required_cols = {"Article", "Date", "Heading", "NewsType"}
        if not required_cols.issubset(df.columns):
            log.error(
                f"CSV missing required columns: {required_cols - set(df.columns)}"
            )
            return
        df = df.dropna(subset=["Article"])
        df = df.tail(1000)  # Use last 1000 news articles

        def safe_str(val):
            if isinstance(val, bytes):
                return val.decode("utf-8", errors="replace")
            return str(val)

        for _, row in df.iterrows():
            article_msg = {
                "article_id": safe_str(row["Article"]),
                "title": safe_str(row["Heading"]),
                "content": safe_str(row["Article"]),
                "author": safe_str(row["NewsType"]),
                "published_at": safe_str(row["Date"]),
            }
            producer.send(topic, value=article_msg)
            log.info(f"Dispatched: {article_msg}")
            time.sleep(0.5)
    except FileNotFoundError:
        log.error(f"CSV file not found: {csv_path}")
    except Exception as exc:
        log.error(f"Streaming error: {exc}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    article_producer = initialize_article_producer()
    if article_producer:
        target_topic = "news_articles_stream"
        try:
            # Download news dataset from Kaggle (asad1m9a9h6mood/news-articles)
            dataset_dir = kagglehub.dataset_download("asad1m9a9h6mood/news-articles")
            csv_file = os.path.join(dataset_dir, "Articles.csv")
            publish_news_stream(article_producer, target_topic, csv_file)
        except Exception as exc:
            logging.getLogger("news_article_producer").error(
                f"Dataset download/streaming failed: {exc}"
            )
        finally:
            article_producer.close()
            logging.getLogger("news_article_producer").info("Kafka producer closed.")
