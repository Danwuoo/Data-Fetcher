# 貢獻指南

感謝你有興趣參與本專案！以下說明開發環境設定、提交規則與 Pull Request 流程。

## 開發環境設定
1. 建議使用 **Python 3.12**。
2. 建立虛擬環境後安裝依賴：
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```
3. 開發過程中可執行以下指令確保品質：
   ```bash
   flake8 .
   mypy --strict src/
   pytest
   ```

## 提交規則
- 採用 **Conventional Commits** 規範，例如 `feat`, `fix`, `docs`, `chore` 等前綴。
- 每次提交應清楚說明變更內容，並在提交前確認 lint 及測試皆通過。

## Pull Request 流程
1. 從 `main` 分支建立功能分支並提交變更。
2. Push 到遠端後於 GitHub 建立 Pull Request。
3. 確認 CI 的 lint 與測試皆成功。
4. 至少一位維護者審核後方可合併。

歡迎透過 Issue 提出建議或回報問題，謝謝你的貢獻！
