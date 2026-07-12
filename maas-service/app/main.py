"""MaaS (Model as a Service) REST API.

Serves the scikit-learn sea-temperature classifier trained by ``train_model.py``.
The Analytics microservice calls these endpoints to enrich the sensor stream
with machine-learning predictions.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import List

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

MODEL_DIR = os.environ.get("MODEL_DIR", "model")
MODEL_PATH = os.path.join(MODEL_DIR, "model.joblib")
METRICS_PATH = os.path.join(MODEL_DIR, "metrics.json")

FEATURES = ["temperature", "humidity"]

model = None
metrics: dict = {}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Load the trained model + metrics once, at startup."""
    global model, metrics
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH, encoding="utf-8") as handle:
            metrics = json.load(handle)
    yield


app = FastAPI(
    title="MaaS - Crop Disease Model as a Service",
    description="scikit-learn RandomForest classifier served over REST for the IoT analytics pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)


class Reading(BaseModel):
    """A single point of the sensor time series."""

    temperature: float = Field(..., description="Temperature in Celsius.")
    humidity: float = Field(..., description="Relative humidity in percent.")


class BatchRequest(BaseModel):
    readings: List[Reading]


class Prediction(BaseModel):
    predicted_class: str
    confidence: float
    probabilities: dict
    features: dict


def _predict_frame(readings: List[Reading]) -> List[Prediction]:
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded.")

    frame = pd.DataFrame([[r.temperature, r.humidity] for r in readings], columns=FEATURES)
    classes = list(model.classes_)
    proba = model.predict_proba(frame)
    predicted_idx = np.argmax(proba, axis=1)

    results: List[Prediction] = []
    for row, idx, reading in zip(proba, predicted_idx, readings):
        results.append(
            Prediction(
                predicted_class=str(classes[idx]),
                confidence=round(float(row[idx]), 4),
                probabilities={str(c): round(float(p), 4) for c, p in zip(classes, row)},
                features={"temperature": reading.temperature, "humidity": reading.humidity},
            )
        )
    return results


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_loaded": model is not None}


@app.get("/model/info")
def model_info() -> dict:
    if not metrics:
        raise HTTPException(status_code=503, detail="Model metrics are not available.")
    return metrics


@app.post("/predict", response_model=Prediction)
def predict(reading: Reading) -> Prediction:
    return _predict_frame([reading])[0]


@app.post("/predict/batch", response_model=List[Prediction])
def predict_batch(request: BatchRequest) -> List[Prediction]:
    if not request.readings:
        raise HTTPException(status_code=400, detail="readings must not be empty.")
    return _predict_frame(request.readings)
