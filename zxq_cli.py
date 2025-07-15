#!/usr/bin/env python
"""ZXQ 指令列工具。"""

from __future__ import annotations

import argparse
from pathlib import Path
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
        print("找不到任何快照")
        return
    latest = snapshots[0]
    print(f"還原 {latest.name}...")
    restore_snapshot(latest)
    print("還原完成")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="zxq")
    subparsers = parser.add_subparsers(dest="command")

    backup_parser = subparsers.add_parser("backup")
    backup_sub = backup_parser.add_subparsers(dest="subcommand")

    verify_parser = backup_sub.add_parser("verify")
    verify_parser.add_argument(
        "--latest",
        action="store_true",
        help="還原最新快照",
    )

    args = parser.parse_args(argv)

    if args.command == "backup" and args.subcommand == "verify":
        if args.latest:
            verify_latest()
        else:
            verify_parser.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
