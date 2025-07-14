from __future__ import annotations

import duckdb
import pandas as pd


class MetadataCatalog:
    """簡易 DuckDB metadata catalog."""

    def __init__(self, path: str = ":memory:"):
        self.con = duckdb.connect(path)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog(
                table_name TEXT,
                tier TEXT,
                location TEXT,
                format TEXT,
                schema_hash TEXT,
                partition_keys TEXT,
                version INTEGER,
                row_count INTEGER
            )
            """
        )

    def upsert(self, row: dict) -> None:
        df = pd.DataFrame([row])
        self.con.execute("DELETE FROM catalog WHERE table_name = ?", (row["table_name"],))
        self.con.register("_tmp", df)
        self.con.execute("INSERT INTO catalog SELECT * FROM _tmp")
        self.con.unregister("_tmp")

    def fetch(self, table_name: str) -> dict | None:
        df = self.con.execute("SELECT * FROM catalog WHERE table_name = ?", (table_name,)).fetch_df()
        return df.iloc[0].to_dict() if not df.empty else None
