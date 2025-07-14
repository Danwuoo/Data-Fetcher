import asyncio
from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache
from data_ingestion.py.data_source import APIDataSource

async def main():
    # Configure the rate limiter: 5 calls per 1 second
    rate_limiter = RateLimiter(calls=5, period=1)

    # Configure the cache: 100 items capacity
    cache = LRUCache(capacity=100)

    # 使用 context manager 建立 API client
    async with ApiClient(base_url="https://jsonplaceholder.typicode.com") as api_client:
        api_source = APIDataSource(
            api_client=api_client,
            rate_limiter=rate_limiter,
            cache=cache,
            endpoint="todos"
        )

        tasks = [api_source.read(params={"id": i}) for i in range(1, 11)]
        results = await asyncio.gather(*tasks)

        for result in results:
            print(result)

if __name__ == "__main__":
    asyncio.run(main())
