import requests
from data_ingestion.rate_limiting.token_bucket import TokenBucket
from data_ingestion.utils.retry import retry_on_exception

class APIDataSource:
    def __init__(self, session, limiter, endpoint):
        self.session = session
        self.limiter = limiter
        self.endpoint = endpoint

    @retry_on_exception
    def call_api(self, params):
        self.limiter.check_rate()
        response = self.session.get(self.endpoint, params=params)
        response.raise_for_status()
        return response

    def read(self, params):
        resp = self.call_api(params)
        return resp.json()
