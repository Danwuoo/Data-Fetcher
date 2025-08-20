# 技術報告：`Backtest-Data-Module` 專案分析與修正建議

## 1. 系統結構 (System Structure)

本專案 `backtest-data-module` 是一個為量化金融設計的 Python 函式庫，其核心目標是提供資料處理、回測與績效分析的工具。

其主要結構特點如下：

*   **核心原始碼**: 專案的主要邏輯都封裝在 `src/backtest_data_module` 目錄下，使其可以作為一個標準的 Python 套件被安裝和使用。
*   **向量化回測引擎**: 系統的回測引擎 (`backtesting/engine.py`) 設計上採用「向量化」模式。這意味著它會一次性將所有歷史資料傳遞給策略進行運算，而不是逐一時間點 (bar-by-bar) 進行模擬。這種設計在利用 `polars` 或 `numpy` 進行高效能運算時有優勢，但目前引擎的實作似乎尚未完全成熟。
*   **策略介面**: 專案定義了一個清晰的策略基礎類別 `backtesting.strategy.StrategyBase`。所有使用者自訂的策略都應繼承此類別，並實作其核心的 `on_data` 方法。
*   **代理模組模式 (Proxy Modules)**: 專案在根目錄下建立了一些與 `src/` 內模組同名的目錄（如 `data_ingestion`）。這些目錄下的 `__init__.py` 檔案透過修改 `sys.modules` 來將匯入路徑代理到 `src/` 內的真實模組。此設計的**初衷可能是為了讓開發者在測試或寫腳本時可以使用較短的匯入路徑**（例如 `from data_ingestion import ...` 而不是 `from backtest_data_module.data_ingestion import ...`），但這個設計**非常脆弱且不符合常規，是導致目前測試失敗的主要原因**。

## 2. 模組位置 (Module Locations)

以下是專案中幾個關鍵模組的路徑及其功能：

*   `src/backtest_data_module/`
    *   **`backtesting/`**: 回測核心功能所在。
        *   `strategy.py`: 定義了策略的抽象基礎類別 `StrategyBase`，是所有策略的入口。
        *   `engine.py`: 包含了 `Backtest` 類別，是回測引擎的主體。
        *   `portfolio.py`, `execution.py`, `performance.py`: 分別負責投資組合管理、交易執行模擬和績效計算。
    *   **`data_ingestion/`**: 負責從外部 API 獲取資料的模組。
    *   **`data_storage/`**: 負責資料儲存與讀取的模組，定義了儲存的抽象介面。
    *   **`data_processing/`**: 包含資料清洗、特徵工程等處理邏輯。
    *   **`zxq/`**: `zxq` CLI 工具的原始碼。
*   `data_ingestion/` (位於根目錄)
    *   這是一個代理模組，其目的是將 `import data_ingestion` 的路徑指向 `src/backtest_data_module/data_ingestion`。**這是問題的來源之一**。
*   `tests/`
    *   包含了專案所有的單元測試和整合測試。**目前這些測試因匯入問題而無法運行**。
*   `my_strategy_guide.py` (我為您建立的檔案)
    *   這是一個完整的、可執行的範例，展示了如何基於此專案的架構撰寫策略和新增技術指標。

## 3. 修正建議 (Correction Suggestions)

當前專案的主要問題在於其混亂且脆弱的匯入結構，導致內建的測試套件完全無法運行。

### 主要問題：循環匯入與代理模組

目前的代理模組模式導致了嚴重的循環匯入問題。簡化後的匯入鏈如下：
1.  測試檔案嘗試 `from data_ingestion import ...`。
2.  這會執行根目錄的 `data_ingestion/__init__.py`。
3.  這個代理 `__init__.py` 為了能代理，它需要先 `from backtest_data_module import data_ingestion as _di` 來取得真實的模組。
4.  這會開始載入 `src/backtest_data_module/data_ingestion/__init__.py`。
5.  在真實模組的載入過程中，它可能又會匯入其他模組（例如 `proxy`），而這些模組中的程式碼又不小心再次 `from data_ingestion import ...`，從而回頭匯入了第一步的代理模組。
6.  此時代理模組尚未完成初始化，導致 Python 拋出 `AttributeError: partially initialized module ...` 的錯誤。

### 修正建議

#### 建議 1: (推薦) 移除所有代理模組
這是最徹底、最符合 Python 開發規範的作法。

*   **行動**:
    1.  刪除專案根目錄下的 `data_ingestion`, `metrics` 等所有代理模組目錄。
    2.  全面搜尋並修改 `tests/` 和 `src/` 目錄下所有的 `import` 語句，將所有類似 `from data_ingestion import ...` 的語句，改為標準的絕對路徑匯入 `from backtest_data_module.data_ingestion import ...`。
*   **優點**:
    *   徹底解決循環匯入問題。
    *   使專案結構清晰、可預測，方便未來維護和新成員的加入。
    *   IDE 和靜態分析工具能更好地理解程式碼。
*   **缺點**:
    *   需要修改大量檔案的 `import` 語句，工作量較大。

#### 建議 2: 完善代理模組（短期作法，不推薦）
如果只是想讓測試暫時跑起來，可以繼續沿用這個有問題的模式。

*   **行動**:
    1.  在根目錄下，仿照 `data_ingestion` 的作法，建立一個 `data_storage/__init__.py` 的代理模組，以解決 `ModuleNotFoundError: No module named 'data_storage'` 的問題。
    2.  仔細調整 `src/` 內各個 `__init__.py` 的匯入順序，嘗試打破循環匯入。這通常非常困難且不可靠。
*   **優點**:
    *   改動範圍可能較小。
*   **缺點**:
    *   治標不治本，未來極有可能再次因微小的改動而出現新的循環匯入問題。

#### 建議 3: 統一程式碼風格
專案目前沒有統一的程式碼風格，`flake8` 會回報大量錯誤。

*   **行動**:
    *   引入 `black` 和 `isort` 等程式碼自動格式化工具。
    *   設定 `pre-commit` hook，在每次提交程式碼時自動格式化，確保風格一致。
*   **優點**:
    *   提升程式碼可讀性，降低維護成本。

#### 建議 4: 完善回測引擎
`Backtest.run` 的實作似乎不完整，難以直接使用。

*   **行動**:
    *   重新審視 `Backtest.run` 的邏輯，使其能真正支援一個清晰的事件驅動或向量化回測流程。
    *   提供更多如 `my_strategy_guide.py` 中的端到端 (end-to-end) 範例，展示如何正確使用引擎。
