import sqlite3
from dataclasses import dataclass


@dataclass
class CatalogEntry:
    """Catalog 資料結構。"""

    table_name: str
    tier: str
    location: str
    schema_hash: str


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
                schema_hash TEXT
            )
            """
        )
        self.conn.commit()

    def upsert(self, entry: CatalogEntry) -> None:
        """新增或更新表格資訊。"""
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO catalog (table_name, tier, location, schema_hash)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    tier=excluded.tier,
                    location=excluded.location,
                    schema_hash=excluded.schema_hash
                """,
                (entry.table_name, entry.tier, entry.location, entry.schema_hash),
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
                "SELECT table_name, tier, location, schema_hash "
                "FROM catalog WHERE table_name=?"
            ),
            (table_name,),
        )
        row = cur.fetchone()
        if row:
            return CatalogEntry(*row)
        return None
