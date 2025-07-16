from typer.testing import CliRunner
from backtest_data_module.zxq import app
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


def test_storage_migrate_dry_run(tmp_path):
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
    result = runner.invoke(
        app,
        [
            "storage",
            "migrate",
            "--table",
            "tbl",
            "--to",
            "warm",
            "--db",
            str(db_path),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "預計將 tbl 從 hot 移至 warm" in result.stdout
