from __future__ import annotations

from typing import Any

import boto3
import duckdb
import psycopg


def init_duck(path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """初始化 DuckDB 資料庫"""
    conn = duckdb.connect(path)
    return conn


def init_timescale(dsn: str) -> psycopg.Connection:
    """初始化 PostgreSQL/TimescaleDB"""
    conn = psycopg.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    conn.commit()
    return conn


def ensure_bucket(bucket: str, *, client: Any | None = None) -> None:
    """確認 S3 bucket 存在，若無則建立"""
    s3 = client or boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
