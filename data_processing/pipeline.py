import csv
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from zxq.pipeline.pipeline_step import PipelineStep
from metrics import PROCESSING_STEP_COUNTER


class Pipeline:
    """
    A data processing pipeline.
    """

    def __init__(self, steps: list[PipelineStep], log_path: str | Path | None = None):
        """初始化 Pipeline

        Args:
            steps: PipelineStep 物件列表
            log_path: 記錄檔案路徑，預設為 pipeline_log.csv
        """
        self.steps = steps
        self.log_path = Path(log_path) if log_path else Path("pipeline_log.csv")

    def process(self, df: pd.DataFrame, num_workers: int = 1) -> pd.DataFrame:
        """依序執行 Pipeline 中的 Step，可選擇平行處理資料批次並記錄耗時與筆數

        Args:
            df: 要處理的 DataFrame
            num_workers: 分批平行處理時的工作數量，1 代表不平行

        Returns:
            處理完成的 DataFrame
        """
        log_exists = self.log_path.exists()
        with open(self.log_path, "a", newline="") as f:
            writer = csv.writer(f)
            if not log_exists:
                writer.writerow(
                    [
                        "timestamp",
                        "step",
                        "input_rows",
                        "output_rows",
                        "duration_s",
                    ]
                )

            for step in self.steps:
                input_rows = len(df)
                start = time.time()

                if num_workers > 1:
                    parts = list(np.array_split(df, num_workers))
                    with ThreadPoolExecutor(max_workers=num_workers) as ex:
                        parts = list(ex.map(step.process, parts))
                    df = pd.concat(parts, ignore_index=True)
                else:
                    df = step.process(df)

                PROCESSING_STEP_COUNTER.labels(step=step.__class__.__name__).inc()

                duration = time.time() - start
                writer.writerow([
                    datetime.now().isoformat(),
                    step.__class__.__name__,
                    input_rows,
                    len(df),
                    f"{duration:.6f}",
                ])
                f.flush()

        return df
