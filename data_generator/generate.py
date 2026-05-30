import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import io
import sys
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "sales-data")


def generate_data(n=1000):
    base_time = datetime.now() - timedelta(days=30)
    timestamps = [
        base_time + timedelta(minutes=np.random.randint(0, 43200)) for _ in range(n)
    ]
    df = pd.DataFrame(
        {
            "id": range(n),
            "product": np.random.choice(["Widget A", "Widget B", "Widget C"], n),
            "quantity": np.random.randint(1, 10, n),
            "price": np.round(np.random.uniform(5, 100, n), 2),
            "timestamp": sorted(timestamps),
        }
    )
    df["total"] = np.round(df["quantity"] * df["price"], 2)
    return df


def upload_to_minio(df: pd.DataFrame, bucket: str, object_name: str):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_buffer = io.BytesIO(csv_bytes)

    client.put_object(
        bucket,
        object_name,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"Uploaded {object_name} ({len(csv_bytes)} bytes) to MinIO bucket '{bucket}'")


if __name__ == "__main__":
    df = generate_data()
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"sales_{timestamp_str}.csv"

    try:
        upload_to_minio(df, MINIO_BUCKET, object_name)
    except S3Error as e:
        # FIX: was silently falling back to /tmp, hiding upload failures from CI
        print(f"MinIO upload failed: {e}", file=sys.stderr)
        sys.exit(1)
