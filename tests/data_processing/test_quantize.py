import numpy as np
import pytest
from unittest.mock import MagicMock
from backtest_data_module.data_handler import DataHandler


def _setup_handler(monkeypatch):
    handler = DataHandler(MagicMock())
    # 將 cupy 模組替換為 numpy，以確保測試環境一致
    monkeypatch.setattr("backtest_data_module.data_handler.cp", np)
    return handler


@pytest.mark.parametrize("bits, dtype", [(8, np.int8), (16, np.int16), (32, np.int32)])
def test_quantize_supported_bits(monkeypatch, bits, dtype):
    handler = _setup_handler(monkeypatch)
    arr = np.array([1, 2, 3], dtype=np.float32)
    result = handler.quantize(arr, bits=bits)
    assert result.dtype == dtype


def test_quantize_invalid_bits(monkeypatch):
    handler = _setup_handler(monkeypatch)
    arr = np.array([1, 2, 3], dtype=np.float32)
    with pytest.raises(ValueError):
        handler.quantize(arr, bits=7)
