import io
import os
import logging

import pandas as pd
import psycopg2
from minio import Minio
from minio.error import S3Error

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

log = logging.getLogger(__name__)

# ── Connection config (injected via docker-compose environment) ───────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "sales-data")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")

STAGED_PATH = "/tmp/staged.csv"
TRANSFORMED_PATH = "/tmp/transformed.csv"


def _minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


# ── Task 1: Extract ───────────────────────────────────────────────────────────
def extract(**context):
    """Download the most recent CSV from MinIO and stage it locally."""
    client = _minio_client()

    try:
        objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
    except S3Error as e:
        raise RuntimeError(f"Cannot list MinIO bucket '{MINIO_BUCKET}': {e}")

    if not objects:
        raise FileNotFoundError(f"No files found in MinIO bucket '{MINIO_BUCKET}'")

    # Pick the most recently modified object
    latest = max(objects, key=lambda o: o.last_modified)
    log.info("Downloading %s from MinIO bucket %s", latest.object_name, MINIO_BUCKET)

    response = client.get_object(MINIO_BUCKET, latest.object_name)
    df = pd.read_csv(io.BytesIO(response.read()))
    df.to_csv(STAGED_PATH, index=False)

    log.info("Staged %d rows to %s", len(df), STAGED_PATH)
    context["ti"].xcom_push(key="source_object", value=latest.object_name)


# ── Task 2: Transform ─────────────────────────────────────────────────────────
def transform(**context):
    """Clean data and add a derived profit column."""
    df = pd.read_csv(STAGED_PATH)

    before = len(df)
    df = df.dropna()
    df = df[df["quantity"] > 0]
    df = df[df["price"] > 0]
    after = len(df)
    log.info("Dropped %d invalid rows (%d → %d)", before - after, before, after)

    df["profit"] = (df["total"] * 0.2).round(2)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df.to_csv(TRANSFORMED_PATH, index=False)
    log.info("Transformed data written to %s", TRANSFORMED_PATH)


# ── Task 3: Load ──────────────────────────────────────────────────────────────
def load(**context):
    """Upsert transformed rows into PostgreSQL."""
    df = pd.read_csv(TRANSFORMED_PATH)

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    # Table is created by config/init.sql but we guard here too
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id          INT,
            product     TEXT,
            quantity    INT,
            price       FLOAT,
            timestamp   TIMESTAMP,
            total       FLOAT,
            profit      FLOAT,
            ingested_at TIMESTAMP DEFAULT NOW()
        )
    """)

    inserted = 0
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO sales (id, product, quantity, price, timestamp, total, profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (
                int(row["id"]),
                str(row["product"]),
                int(row["quantity"]),
                float(row["price"]),
                row["timestamp"],
                float(row["total"]),
                float(row["profit"]),
            ),
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    log.info("Loaded %d rows into postgres.sales", inserted)
    context["ti"].xcom_push(key="rows_loaded", value=inserted)


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="mini_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    t3 = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t1 >> t2 >> t3
