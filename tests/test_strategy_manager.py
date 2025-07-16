import os
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import pytest
from fastapi import HTTPException

from backtest_data_module.strategy_manager.main import app, get_db
from backtest_data_module.strategy_manager.database import Base, aget_db
from backtest_data_module.strategy_manager.auth import get_api_key as real_get_api_key

SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

async def override_get_api_key():
    pass


app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[aget_db] = override_get_db
app.dependency_overrides[real_get_api_key] = override_get_api_key


client = TestClient(app)

@pytest.fixture(scope="function", autouse=True)
def setup_teardown():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def test_create_run():
    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["strategy_name"] == "test_strategy"
    assert "run_id" in data


def test_read_run():
    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    response = client.get(f"/runs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run_id


def test_read_runs():
    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy_1",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200
    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy_2",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200

    response = client.get("/runs")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_update_run():
    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    response = client.put(
        f"/runs/{run_id}",
        json={"status": "COMPLETED", "metrics_uri": "/path/to/metrics.json"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert data["metrics_uri"] == "/path/to/metrics.json"


def test_auth_no_api_key(monkeypatch):
    monkeypatch.setenv("STRATEGY_MANAGER_API_KEY", "test_key")

    def mock_get_api_key_unauthorized():
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")

    app.dependency_overrides[real_get_api_key] = mock_get_api_key_unauthorized

    response = client.post(
        "/runs",
        json={
            "strategy_name": "test_strategy",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 401
    app.dependency_overrides[real_get_api_key] = override_get_api_key


def test_auth_with_api_key(monkeypatch):
    monkeypatch.setenv("STRATEGY_MANAGER_API_KEY", "test_key")

    app.dependency_overrides[real_get_api_key] = real_get_api_key

    response = client.post(
        "/runs",
        headers={"X-API-KEY": "test_key"},
        json={
            "strategy_name": "test_strategy",
            "strategy_version": "1.0",
            "hyperparameters": {"param1": "value1"},
            "orchestrator_type": "WFA",
        },
    )
    assert response.status_code == 200
    app.dependency_overrides[real_get_api_key] = override_get_api_key
