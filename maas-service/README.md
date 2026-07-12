# MaaS Microservice (Model as a Service)

Python + **FastAPI** microservice that serves a **scikit-learn** machine-learning
model over REST. It is consumed by the Analytics microservice to enrich the
sensor stream with predictions (Project 3, requirement 1b & 3).

## Model

- **Algorithm:** `RandomForestClassifier` inside a `StandardScaler` pipeline.
- **Task:** multi-class classification of `sea_temp_band` — the sea-temperature
  band (`cold`, `mild`, `warm`) of the ocean.
- **Features:** air `temperature` and `humidity` — exactly the two values that
  travel through the MQTT pipeline as a time series, so the model can score live
  events.
- **Data:** `marine_buoy_readings.csv` — real hourly observations from Irish
  Marine Institute weather buoys (8 stations). Air temperature + humidity carry
  genuine signal about sea temperature.
- **Training:** 70/15/15 stratified train/validation/test split
  (`train_model.py`), persisted with `joblib`. Metrics are written to
  `model/metrics.json` and exposed at `/model/info`.
- **Accuracy:** ~78% on the held-out test set (vs. 33% chance for 3 balanced
  classes) — real signal, reported honestly at `/model/info`.

## REST endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness + whether the model is loaded. |
| `GET` | `/model/info` | Model metadata + validation/test metrics. |
| `POST` | `/predict` | Predict one reading `{ "temperature": 34, "humidity": 42 }`. |
| `POST` | `/predict/batch` | Predict `{ "readings": [ {..}, {..} ] }`. |

Interactive docs are available at `/docs` (Swagger UI).

### Example

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"temperature": 34.0, "humidity": 42.0}'
```

```json
{
  "predicted_class": "warm",
  "confidence": 0.86,
  "probabilities": {"cold": 0.05, "mild": 0.09, "warm": 0.86},
  "features": {"temperature": 34.0, "humidity": 42.0}
}
```

## Run locally (without Docker)

```bash
pip install -r requirements.txt
python train_model.py --dataset ../../shared/sample-data/marine_buoy_readings.csv
uvicorn app.main:app --reload
```

## Docker

The image trains the model at build time, so no artefacts need to be committed:

```bash
docker compose build maas-service
```
