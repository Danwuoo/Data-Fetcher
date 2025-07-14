import unittest
import requests_mock
from data_ingestion.py.api_client import ApiClient

class TestApiClient(unittest.TestCase):
    """
    Tests for the ApiClient class.
    """

    def setUp(self):
        self.base_url = "http://test.com"
        self.api_client = ApiClient(base_url=self.base_url)

    @requests_mock.Mocker()
    def test_call_api_success(self, m):
        """
        Tests a successful API call.
        """
        endpoint = "test"
        expected_response = {"data": "success"}
        m.get(f"{self.base_url}/{endpoint}", json=expected_response)

        response = self.api_client.call_api(endpoint)
        self.assertEqual(response, expected_response)

    @requests_mock.Mocker()
    def test_call_api_retry(self, m):
        """
        Tests the retry logic for a 429 error.
        """
        endpoint = "test"
        m.get(f"{self.base_url}/{endpoint}", [
            {"status_code": 429, "headers": {"Retry-After": "1"}},
            {"status_code": 200, "json": {"data": "success"}}
        ])

        response = self.api_client.call_api(endpoint)
        self.assertEqual(response, {"data": "success"})
        self.assertEqual(m.call_count, 2)

if __name__ == "__main__":
    unittest.main()
