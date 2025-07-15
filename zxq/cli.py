import typer
from data_storage import Catalog, CatalogEntry

app = typer.Typer(help="ZXQuant CLI 工具")

audit_app = typer.Typer(help="稽核相關指令")
app.add_typer(audit_app, name="audit")


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


if __name__ == "__main__":
    app()
