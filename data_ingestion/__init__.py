from __future__ import annotations

import sys

from backtest_data_module import data_ingestion as _ing
from backtest_data_module.data_ingestion import py as _py

# 須先註冊 py 子模組，供其餘模組匯入使用
sys.modules[__name__ + '.py'] = _py

from backtest_data_module.data_ingestion import proxy as _proxy
from backtest_data_module.data_ingestion import metrics as _metrics

# 導入並重新導出函式
from backtest_data_module.data_ingestion import *  # noqa:F401,F403

# 註冊子模組，確保路徑相容
sys.modules[__name__ + '.proxy'] = _proxy
sys.modules[__name__ + '.metrics'] = _metrics
