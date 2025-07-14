import pytest
from httpx import Response
from data_ingestion.py.api_client import ApiClient

@pytest.mark.asyncio
async def test_call_api_success(httpx_mock):
    """
    Tests a successful API call.
    """
    base_url = "http://test.com"
    endpoint = "test"
    expected_response = {"data": "success"}
    httpx_mock.add_response(url=f"{base_url}/{endpoint}", json=expected_response)
    api_client = ApiClient(base_url=base_url)

    response = await api_client.call_api(endpoint)
    assert response == expected_response

@pytest.mark.asyncio
async def test_call_api_retry(httpx_mock):
    """
    Tests the retry logic for a 429 error.
    """
    base_url = "http://test.com"
    endpoint = "test"
    httpx_mock.add_callback(
        lambda request, ext: Response(429, headers={"Retry-After": "0.1"}),
    )
    httpx_mock.add_response(url=f"{base_url}/{endpoint}", json={"data": "success"})
    api_client = ApiClient(base_url=base_url)

    response = await api_client.call_api(endpoint)
    assert response == {"data": "success"}
    assert len(httpx_mock.get_requests()) == 2
