import typer
from pathlib import Path
from data_processing.cross_validation import walk_forward_split
from data_storage import Catalog, CatalogEntry, HybridStorageManager
import shutil

SNAPSHOT_DIR = Path("snapshots")
RESTORE_DIR = Path("restored")


def restore_snapshot(snapshot: Path) -> None:
    """還原指定快照檔案。"""
    if not snapshot.exists():
        raise FileNotFoundError(snapshot)
    RESTORE_DIR.mkdir(exist_ok=True)
    shutil.unpack_archive(str(snapshot), str(RESTORE_DIR))


def verify_latest() -> None:
    """尋找並還原最新的快照。"""
    snapshots = sorted(
        SNAPSHOT_DIR.glob("*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not snapshots:
        typer.echo("找不到任何快照")
        return
    latest = snapshots[0]
    typer.echo(f"還原 {latest.name}...")
    restore_snapshot(latest)
    typer.echo("還原完成")


app = typer.Typer(help="ZXQuant CLI 工具")

audit_app = typer.Typer(help="稽核相關指令")
app.add_typer(audit_app, name="audit")

storage_app = typer.Typer(help="儲存相關指令")
app.add_typer(storage_app, name="storage")

backup_app = typer.Typer(help="備份還原指令")
app.add_typer(backup_app, name="backup")


@audit_app.command()
def trace(table: str, db: str = ":memory:") -> None:
    """從 Catalog 讀取表格所在層級與位置。"""
    catalog = Catalog(db_path=db)
    entry: CatalogEntry | None = catalog.get(table)
    if not entry:
        typer.echo(f"找不到表格 {table}")
        raise typer.Exit(code=1)

    msg = (
        f"表格 {entry.table_name} 位於 {entry.tier} (位置: {entry.location})\n"
        f"Schema: {entry.schema_hash}"
    )
    typer.echo(msg)


@app.command()
def walk_forward(
    samples: int,
    train_size: int,
    test_size: int,
    step_size: int,
) -> None:
    """Walk-Forward 資料切分。"""
    for train_idx, test_idx in walk_forward_split(
        samples, train_size, test_size, step_size
    ):
        typer.echo(f"{train_idx} {test_idx}")


@storage_app.command()
def migrate(
    table: str = typer.Option(..., "--table", help="要移動的資料表"),
    to: str = typer.Option(..., "--to", help="目標層級"),
    db: str = typer.Option(":memory:", "--db", help="Catalog 位置"),
    dry_run: bool = typer.Option(False, "--dry-run", help="僅顯示預計行動"),
) -> None:
    """移動資料表至指定層級。"""
    manager = HybridStorageManager(catalog=Catalog(db_path=db))
    entry = manager.catalog.get(table)
    if entry is None:
        typer.echo(f"找不到資料表 {table}")
        raise typer.Exit(code=1)
    if dry_run:
        typer.echo(f"預計將 {table} 從 {entry.tier} 移至 {to}")
    else:
        manager.migrate(table, entry.tier, to)
        typer.echo(f"已將 {table} 從 {entry.tier} 移至 {to}")


@backup_app.command()
def verify(latest: bool = False) -> None:
    """驗證或還原備份。"""
    if latest:
        verify_latest()
    else:
        typer.echo("請使用 --latest 參數")


if __name__ == "__main__":
    app()
