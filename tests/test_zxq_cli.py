from typer.testing import CliRunner
from zxq import app
from data_storage import Catalog, CatalogEntry


def test_audit_trace(tmp_path):
    db_path = tmp_path / "cat.db"
    catalog = Catalog(db_path=str(db_path))
    catalog.upsert(
        CatalogEntry(
            table_name="tbl",
            version=1,
            tier="hot",
            location="hot",
            schema_hash="h",
            partition_keys="",
        )
    )

    runner = CliRunner()
    result = runner.invoke(app, ["audit", "trace", "tbl", "--db", str(db_path)])
    assert result.exit_code == 0
    assert "tbl" in result.stdout
