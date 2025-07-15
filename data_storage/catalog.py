import sqlite3
import hashlib
from dataclasses import dataclass


@dataclass
class CatalogEntry:
    """Catalog 資料結構。"""

    table_name: str
    tier: str
    location: str
    schema_hash: str
    row_count: int = 0
    lineage: str = ""


class Catalog:
    """使用 SQLite 紀錄資料表所在層級與 schema。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog (
                table_name TEXT PRIMARY KEY,
                tier TEXT,
                location TEXT,
                schema_hash TEXT,
                row_count INTEGER DEFAULT 0,
                lineage TEXT DEFAULT ""
            )
            """
        )
        self.conn.commit()

    def upsert(self, entry: CatalogEntry) -> None:
        """新增或更新表格資訊。"""
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO catalog (
                    table_name, tier, location, schema_hash, row_count, lineage
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    tier=excluded.tier,
                    location=excluded.location,
                    schema_hash=excluded.schema_hash,
                    row_count=excluded.row_count,
                    lineage=excluded.lineage
                """,
                (
                    entry.table_name,
                    entry.tier,
                    entry.location,
                    entry.schema_hash,
                    entry.row_count,
                    entry.lineage,
                ),
            )

    def update_tier(self, table_name: str, tier: str, location: str) -> None:
        """更新表格所在層級。"""
        with self.conn:
            self.conn.execute(
                "UPDATE catalog SET tier=?, location=? WHERE table_name=?",
                (tier, location, table_name),
            )

    def get(self, table_name: str) -> CatalogEntry | None:
        cur = self.conn.execute(
            (
                "SELECT table_name, tier, location, schema_hash, row_count, lineage "
                "FROM catalog WHERE table_name=?"
            ),
            (table_name,),
        )
        row = cur.fetchone()
        if row:
            return CatalogEntry(*row)
        return None


def send_slack_alert(message: str, webhook_url: str | None = None) -> None:
    """傳送 Slack 警報，webhook 未設定則忽略。"""
    if not webhook_url:
        return
    try:
        import httpx

        httpx.post(webhook_url, json={"text": message})
    except Exception:
        pass


def check_drift(manager, webhook_url: str | None = None) -> list[str]:
    """重新計算 schema hash，若不一致則警告並更新紀錄。"""
    from .storage_backend import HybridStorageManager

    if not isinstance(manager, HybridStorageManager):
        raise TypeError("manager must be HybridStorageManager")

    catalog = manager.catalog
    cur = catalog.conn.execute("SELECT table_name, tier, schema_hash FROM catalog")
    mismatches = []
    for table, tier, stored_hash in cur.fetchall():
        backend = manager._backend_for(tier)
        try:
            df = backend.read(table)
        except KeyError:
            continue
        new_hash = hashlib.sha256(str(df.dtypes.to_dict()).encode()).hexdigest()
        catalog.conn.execute(
            "UPDATE catalog SET schema_hash=?, row_count=? WHERE table_name=?",
            (new_hash, len(df), table),
        )
        if new_hash != stored_hash:
            mismatches.append(table)
            send_slack_alert(f"Schema drift detected for {table}", webhook_url)
    catalog.conn.commit()
    return mismatches
