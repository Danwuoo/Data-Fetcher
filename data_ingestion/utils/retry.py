from tenacity import retry, wait_random_exponential, stop_after_attempt
import requests

def is_retryable_error(exception):
    """Return True if we should retry, False otherwise"""
    if isinstance(exception, requests.exceptions.HTTPError):
        # Retry on 429 Too Many Requests and 5xx server errors.
        return exception.response.status_code == 429 or exception.response.status_code >= 500
    return False

retry_on_exception = retry(
    wait=wait_random_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(6),
    retry=is_retryable_error
)
