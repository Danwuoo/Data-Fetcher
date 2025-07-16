from __future__ import annotations

import sys

from backtest_data_module import utils as _utils

# 匯入並重新導出模組內容
from backtest_data_module.utils.notify import *  # noqa:F401,F403
from backtest_data_module.utils.json_encoder import *  # noqa:F401,F403
# 讓 `utils.notify` 與 `utils.json_encoder` 等路徑能正確解析
sys.modules[__name__ + '.notify'] = _utils.notify
sys.modules[__name__ + '.json_encoder'] = _utils.json_encoder
