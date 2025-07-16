import datetime as dt
from sqlalchemy import Column, DateTime, Enum, JSON, String
from sqlalchemy.dialects.postgresql import UUID
import uuid
from .database import Base

class Run(Base):
    __tablename__ = "runs"

    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=dt.datetime.utcnow)
    strategy_name = Column(String, nullable=False)
    strategy_version = Column(String, nullable=False)
    hyperparameters = Column(JSON, nullable=False)
    orchestrator_type = Column(String, nullable=False)
    metrics_uri = Column(String, nullable=True)
    status = Column(Enum("PENDING", "RUNNING", "FAILED", "COMPLETED", name="run_status"), nullable=False)
    error_message = Column(String, nullable=True)
