import httpx
import asyncio
from tenacity import retry, wait_random_exponential, stop_after_attempt

class ApiClient:
    """
    A simple asynchronous API client with retry logic.
    """

    def __init__(self, base_url: str):
        """
        Initializes the ApiClient.

        Args:
            base_url: The base URL for the API.
        """
        self.base_url = base_url
        self.session = httpx.AsyncClient()

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
