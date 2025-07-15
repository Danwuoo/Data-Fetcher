from locust import HttpUser, task, constant_pacing


class ProxyUser(HttpUser):
    """簡單的 proxy 壓力測試使用者，每秒發送一次請求。"""

    wait_time = constant_pacing(1)

    @task
    def fetch_todo(self):
        """向 proxy 取得範例待辦事項。"""
        self.client.get("/todos/1")
