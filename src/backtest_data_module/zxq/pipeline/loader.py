import importlib
import sys
from typing import List, Type

import yaml

from zxq.pipeline.pipeline_step import PipelineStep


def load_steps_from_yaml(path: str) -> List[Type[PipelineStep]]:
    """從 YAML 讀取 Step class 路徑並回傳類別列表。"""
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    steps = []
    for entry in config.get("steps", []):
        module_path, class_name = entry.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ImportError(f"找不到 {entry}")
        if not issubclass(cls, PipelineStep):
            raise TypeError(f"{entry} 不是 PipelineStep 的子類別")
        steps.append(cls)
    return steps


def main(argv: List[str] | None = None) -> None:
    path = argv[1] if argv and len(argv) > 1 else "steps.yaml"
    load_steps_from_yaml(path)


if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception as e:
        print(e)
        raise
