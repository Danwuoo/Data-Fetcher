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

import argparse
from data_processing.cross_validation import walk_forward_split


def _cmd_walk_forward(args: argparse.Namespace) -> None:
    """執行 Walk-Forward 切分並列印索引。"""
    for train_idx, test_idx in walk_forward_split(
        args.samples,
        args.train_size,
        args.test_size,
        args.step_size,
    ):
        print(train_idx, test_idx)


def main() -> None:
    parser = argparse.ArgumentParser(prog="zxq")
    subparsers = parser.add_subparsers(dest="command")

    wf = subparsers.add_parser("walk-forward", help="Walk-Forward 資料切分")
    wf.add_argument("samples", type=int, help="總樣本數")
    wf.add_argument("train_size", type=int, help="訓練集大小")
    wf.add_argument("test_size", type=int, help="測試集大小")
    wf.add_argument("step_size", type=int, help="步進大小")
    wf.set_defaults(func=_cmd_walk_forward)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
