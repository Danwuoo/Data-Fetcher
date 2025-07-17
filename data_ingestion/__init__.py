from __future__ import annotations

import sys

from backtest_data_module.data_ingestion import py as _py
from backtest_data_module.data_ingestion import proxy as _proxy
from backtest_data_module.data_ingestion import metrics as _metrics
from backtest_data_module.data_ingestion import *  # noqa: F401,F403

# 先註冊子模組，讓其餘模組可以順利匯入
sys.modules[__name__ + ".py"] = _py
sys.modules[__name__ + ".proxy"] = _proxy
sys.modules[__name__ + ".metrics"] = _metrics
