import pandas as pd
import pytest

from data_storage import DuckHot, TimescaleWarm, S3Cold, HybridStorageManager


@pytest.mark.parametrize("backend_cls", [DuckHot, TimescaleWarm, S3Cold])
def test_backend_write_read_delete(backend_cls):
    backend = backend_cls()
    df = pd.DataFrame({"a": [1, 2]})
    backend.write(df, "tbl")
    result = backend.read("tbl")
    pd.testing.assert_frame_equal(result, df)

    backend.delete("tbl")
    with pytest.raises(KeyError):
        backend.read("tbl")


def test_manager_invalid_tier():
    manager = HybridStorageManager()
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        manager.write(df, "tbl", tier="unknown")


def test_manager_read_missing():
    manager = HybridStorageManager()
    with pytest.raises(KeyError):
        manager.read("missing")
