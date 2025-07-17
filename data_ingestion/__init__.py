from __future__ import annotations

import sys

from backtest_data_module import data_ingestion as _di

# 先註冊子模組，避免相依模組匯入失敗
sys.modules[__name__ + ".py"] = _di.py
sys.modules[__name__ + ".proxy"] = _di.proxy
sys.modules[__name__ + ".metrics"] = _di.metrics

from backtest_data_module.data_ingestion import *  # noqa: F401,F403,E402
