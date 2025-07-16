# Strategy Manager API

The Strategy Manager is a FastAPI application that provides a RESTful API for managing and tracking backtest runs.

## Running the API

To run the API, you can use the following command:

```bash
uvicorn backtest_data_module.strategy_manager.main:app --host 0.0.0.0 --port 8000
```

## Authentication

The API uses token-based authentication. To authenticate, you need to provide an API key in the `X-API-KEY` header of your requests. The API key can be set using the `STRATEGY_MANAGER_API_KEY` environment variable.

## API Endpoints

### POST /runs

Create a new backtest run.

**Request Body:**

```json
{
  "strategy_name": "string",
  "strategy_version": "string",
  "hyperparameters": {},
  "orchestrator_type": "string"
}
```

**Response:**

```json
{
  "run_id": "uuid",
  "timestamp": "datetime",
  "strategy_name": "string",
  "strategy_version": "string",
  "hyperparameters": {},
  "orchestrator_type": "string",
  "metrics_uri": "string",
  "status": "string",
  "error_message": "string"
}
```

### GET /runs

Get a list of all backtest runs.

**Query Parameters:**

* `skip`: integer (default: 0)
* `limit`: integer (default: 100)

**Response:**

A list of run objects.

### GET /runs/{run_id}

Get a specific backtest run by its ID.

**Response:**

A run object.

### PUT /runs/{run_id}

Update a backtest run.

**Request Body:**

```json
{
  "status": "string",
  "metrics_uri": "string",
  "error_message": "string"
}
```

**Response:**

The updated run object.

## Orchestrator Integration

The `Orchestrator` class can be configured to automatically register and update runs in the Strategy Manager. To enable this, you need to provide the `register_api` parameter when creating an `Orchestrator` instance.

```python
from backtest_data_module.backtesting.orchestrator import Orchestrator

orchestrator = Orchestrator(
    ...,
    register_api="http://localhost:8000"
)
```

You also need to set the `STRATEGY_MANAGER_API_KEY` environment variable with your API key.
