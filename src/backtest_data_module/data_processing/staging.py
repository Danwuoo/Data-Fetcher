import pandas as pd
import os


def save_to_staging(df: pd.DataFrame, path: str):
    """將 DataFrame 儲存至暫存區。

    Args:
        df: 要儲存的 DataFrame。
        path: 目標路徑。
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path)


def load_from_staging(path: str) -> pd.DataFrame:
    """從暫存區讀取 DataFrame。

    Args:
        path: 檔案路徑。

    Returns:
        讀取後的 DataFrame。
    """
    return pd.read_parquet(path)
