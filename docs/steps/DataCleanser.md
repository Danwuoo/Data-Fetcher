# DataCleanser

此步驟負責清理資料，可選擇剔除離群值。

## 前後對照範例
```python
import pandas as pd
from data_processing.data_cleanser import DataCleanser

df = pd.DataFrame({'value': [1, 2, 100]})
cleaned = DataCleanser(remove_outliers=True).process(df)
print(cleaned)
```
輸出：
```text
   value
0      1
1      2
```
