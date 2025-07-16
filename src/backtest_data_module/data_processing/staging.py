import pandas as pd
import os


def save_to_staging(df: pd.DataFrame, path: str):
    """
    Saves a DataFrame to the staging area.

    Args:
        df: The DataFrame to save.
        path: The path to save the DataFrame to.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path)


def load_from_staging(path: str) -> pd.DataFrame:
    """
    Loads a DataFrame from the staging area.

    Args:
        path: The path to load the DataFrame from.

    Returns:
        The loaded DataFrame.
    """
    return pd.read_parquet(path)
