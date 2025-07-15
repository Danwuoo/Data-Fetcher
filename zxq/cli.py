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
