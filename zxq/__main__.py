"""zxq 指令列工具。"""

import argparse
from data_storage import HybridStorageManager


def _cmd_storage_migrate(args: argparse.Namespace) -> None:
    manager = HybridStorageManager()
    entry = manager.catalog.get(args.table)
    if entry is None:
        raise SystemExit(f"找不到資料表 {args.table}")
    if args.dry_run:
        print(f"預計將 {args.table} 從 {entry.tier} 移至 {args.to}")
    else:
        manager.migrate(args.table, entry.tier, args.to)
        print(f"已將 {args.table} 從 {entry.tier} 移至 {args.to}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="zxq")
    subparsers = parser.add_subparsers(dest="command")

    storage_parser = subparsers.add_parser("storage")
    storage_sub = storage_parser.add_subparsers(dest="action")

    migrate_parser = storage_sub.add_parser("migrate")
    migrate_parser.add_argument("--table", required=True)
    migrate_parser.add_argument("--to", required=True, choices=["hot", "warm", "cold"])
    migrate_parser.add_argument("--dry-run", action="store_true")
    migrate_parser.set_defaults(func=_cmd_storage_migrate)

    args = parser.parse_args(argv)
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
