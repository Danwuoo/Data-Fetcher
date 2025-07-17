# Backtest-Data-Module

[![CI](https://github.com/Danwuoo/Backtest-Data-Module/actions/workflows/ci.yml/badge.svg)](https://github.com/Danwuoo/Backtest-Data-Module/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Danwuoo/Backtest-Data-Module/branch/main/graph/badge.svg)](https://codecov.io/gh/Danwuoo/Backtest-Data-Module)
[![PyPI](https://img.shields.io/pypi/v/backtest-data-module.svg)](https://pypi.org/project/backtest-data-module/)
[![License](https://img.shields.io/github/license/Danwuoo/Backtest-Data-Module.svg)](LICENSE)

Backtest-Data-Module 提供非同步 API 資料擷取、資料處理流程與回測相關工具，可用於 ZXQuant 或其他量化交易系統。

## 特色

- **非同步資料擷取與快取**：整合多種 API，並提供自動快取機制
- **速率限制與批次調整**：依據目標延遲自動調節批次大小與並發量
- **Prefect 流程管理**：輕鬆建立 ETL 流程與排程任務
- **ZXQuant CLI 工具**：內建 Walk-Forward 切分等實用指令
- **交叉驗證與績效分析**：支援 CPCV 及多種績效指標計算
- **監控與稽核**：提供 Prometheus 指標及資料 lineage 查詢

## 快速開始

### 安裝

建議使用 Python 3.12+，可直接透過 PyPI 安裝：

```bash
pip install backtest-data-module
```

若需在本地執行 `flake8` 或 `pytest` 進行檢查，請先安裝開發依賴：

```bash
pip install -r requirements.txt
```

### 範例

執行隨附的 `example.py` 示範如何擷取資料並寫入快取：

```bash
python example.py
```

或使用 CLI 進行 Walk-Forward 切分：

```bash
zxq walk-forward 10 3 2 2
```

## 版本管理

本專案遵循 [Semantic Versioning](https://semver.org/lang/zh-TW/)。

更多資訊請參閱本倉庫內的文件及 `docs/` 目錄。

想參與開發或回報安全性問題，請參閱 [貢獻指南](CONTRIBUTING.md)、[行為準則](CODE_OF_CONDUCT.md) 與 [安全性政策](SECURITY.md)。
