from fastapi import Depends, FastAPI, HTTPException, Security
from sqlalchemy.orm import Session
from typing import List
import uuid

from . import models, schemas
from .auth import get_api_key
from .database import SessionLocal, engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.post("/runs", response_model=schemas.Run)
def create_run(run: schemas.RunCreate, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    db_run = models.Run(**run.model_dump(), status="PENDING")
    db.add(db_run)
    db.commit()
    db.refresh(db_run)
    return db_run

@app.get("/runs", response_model=List[schemas.Run])
def read_runs(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    runs = db.query(models.Run).offset(skip).limit(limit).all()
    return runs

@app.get("/runs/{run_id}", response_model=schemas.Run)
def read_run(run_id: uuid.UUID, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    db_run = db.query(models.Run).filter(models.Run.run_id == run_id).first()
    if db_run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return db_run

@app.put("/runs/{run_id}", response_model=schemas.Run)
def update_run(run_id: uuid.UUID, run_update: schemas.RunUpdate, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    db_run = db.query(models.Run).filter(models.Run.run_id == run_id).first()
    if db_run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    update_data = run_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_run, key, value)

    db.commit()
    db.refresh(db_run)
    return db_run
