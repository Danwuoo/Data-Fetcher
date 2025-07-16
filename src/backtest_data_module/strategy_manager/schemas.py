import datetime as dt
from pydantic import BaseModel
from uuid import UUID
from typing import Optional, Any

class RunBase(BaseModel):
    strategy_name: str
    strategy_version: str
    hyperparameters: dict[str, Any]
    orchestrator_type: str

class RunCreate(RunBase):
    pass

class RunUpdate(BaseModel):
    status: Optional[str] = None
    metrics_uri: Optional[str] = None
    error_message: Optional[str] = None

class Run(RunBase):
    run_id: UUID
    timestamp: dt.datetime
    status: str
    metrics_uri: Optional[str] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True
