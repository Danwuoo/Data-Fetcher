# TimeAligner

將時間序列對齊並依規則重採樣。

## 前後對照範例
```python
import pandas as pd
from data_processing.time_aligner import TimeAligner

df = pd.DataFrame({
    'timestamp': ['2024-01-01 00:00:00', '2024-01-01 00:00:30'],
    'value': [1, 2]
})
aligned = TimeAligner('1T').process(df)
print(aligned)
```
輸出：
```text
             timestamp  value
0 2024-01-01 00:00:00    1.5
```
