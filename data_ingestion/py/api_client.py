import httpx
import asyncio
from tenacity import retry, wait_random_exponential, stop_after_attempt

class ApiClient:
    """
    A simple asynchronous API client with retry logic.
    """

    def __init__(self, base_url: str):
        """初始化 ApiClient。

        Args:
            base_url: API 的基底網址
        """
        self.base_url = base_url
        self.session: httpx.AsyncClient | None = None

    async def __aenter__(self):
        """建立並回傳非同步 HTTP session。"""
        self.session = httpx.AsyncClient()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """關閉 HTTP session。"""
        if self.session:
            await self.session.aclose()

    @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    async def call_api(self, endpoint: str, params: dict = None):
        """
        Calls an API endpoint asynchronously.

        Args:
            endpoint: The API endpoint to call.
            params: The query parameters for the request.

        Returns:
            The JSON response from the API.
        """
        if self.session is None:
            self.session = httpx.AsyncClient()
        url = f"{self.base_url}/{endpoint}"
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 0))
                if retry_after > 0:
                    await asyncio.sleep(retry_after)
            raise e
