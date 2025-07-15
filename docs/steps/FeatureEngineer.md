# FeatureEngineer

依照指定的功能產生新欄位，目前支援 `moving_average`。

## 前後對照範例
```python
import pandas as pd
from data_processing.feature_engineer import FeatureEngineer

df = pd.DataFrame({'close': [1, 2, 3, 4, 5, 6]})
engineered = FeatureEngineer(['moving_average']).process(df)
print(engineered[['close', 'moving_average']])
```
輸出：
```text
   close  moving_average
0      1             NaN
1      2             NaN
2      3             NaN
3      4             NaN
4      5             3.0
5      6             4.0
```
