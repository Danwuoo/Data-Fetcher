import requests
from data_ingestion.connectors.api_source import APIDataSource
from data_ingestion.rate_limiting.token_bucket import TokenBucket

def main():
    # Configure the rate limiter: 10 requests per 10 seconds
    limiter = TokenBucket(10, 10)

    # Create a session
    session = requests.Session()

    # Create the API data source
    api_source = APIDataSource(
        session=session,
        limiter=limiter,
        endpoint="https://jsonplaceholder.typicode.com/todos/1"
    )

    # Fetch data
    try:
        data = api_source.read(params={})
        print("Successfully fetched data:")
        print(data)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
