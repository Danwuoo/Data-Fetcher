# 貢獻指南


感謝您有興趣參與 Backtest-Data-Module 的開發！本文件說明開發環境設定、提交規範與 PR 流程。

## 開發環境建置
1. 建議使用 **Python 3.12**。
2. 下載專案：
   ```bash
   git clone <repo-url>
   cd Backtest-Data-Module
   ```
3. 安裝依賴：

感謝你有興趣參與本專案！以下說明開發環境設定、提交規則與 Pull Request 流程。

## 開發環境設定
1. 建議使用 **Python 3.12**。
2. 建立虛擬環境後安裝依賴：

   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

   或者使用 Poetry：
   ```bash
   poetry install
   ```

## 提交訊息規範
- 以英文前綴標示變更類型，例如：`feat:`, `fix:`, `docs:`, `chore:`。
- 內容可使用繁體中文或英文，務必簡潔明瞭。

範例：
```
feat: 新增資料快取機制
```

## Pull Request 流程
1. **Fork** 專案並建立分支。
2. 完成開發後推送至個人分支並提出 PR。
3. 變更程式碼時，請在本地先執行：

3. 開發過程中可執行以下指令確保品質：

   ```bash
   flake8 .
   mypy --strict src/
   pytest
   ```

   只修改文件或註解則可略過。
4. PR 描述請清楚列出變更內容與目的。
5. 通過審查及自動化測試後即可合併。

歡迎提出 issue 或 PR，一同讓專案變得更好！


## 提交規則
- 採用 **Conventional Commits** 規範，例如 `feat`, `fix`, `docs`, `chore` 等前綴。
- 每次提交應清楚說明變更內容，並在提交前確認 lint 及測試皆通過。

## Pull Request 流程
1. 從 `main` 分支建立功能分支並提交變更。
2. Push 到遠端後於 GitHub 建立 Pull Request。
3. 確認 CI 的 lint 與測試皆成功。
4. 至少一位維護者審核後方可合併。

歡迎透過 Issue 提出建議或回報問題，謝謝你的貢獻！

