import os
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

API_KEY = os.environ.get("STRATEGY_MANAGER_API_KEY")
API_KEY_NAME = "X-API-KEY"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


async def get_api_key(api_key_header: str = Security(api_key_header)):
    if not API_KEY:
        # If no API key is set in the environment, disable authentication
        return
    if api_key_header == API_KEY:
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
