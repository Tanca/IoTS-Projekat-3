"""Train, validate and test the MaaS sea-temperature classifier.

The model is a scikit-learn pipeline (StandardScaler + RandomForestClassifier)
that predicts the ``sea_temp_band`` (cold / mild / warm) of the ocean from the
two sensor features that flow through the MQTT pipeline as a time series:
air ``temperature`` and ``humidity``. The data are real hourly observations from
Irish Marine Institute weather buoys, so the relationship is genuine (air
temperature + humidity carry real signal about sea temperature).

Running this script produces two artefacts next to the trained model:

* ``model/model.joblib``  - the fitted pipeline, loaded by the FastAPI service.
* ``model/metrics.json``  - metadata + validation/test metrics, exposed by the
  ``/model/info`` endpoint so the numbers are auditable at runtime.

Usage::

    python train_model.py --dataset /path/to/marine_buoy_readings.csv
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# The features are exactly the values that arrive in a SensorReading event, so
# the model can score the live stream without any extra data sources.
FEATURES = ["temperature", "humidity"]
TARGET = "sea_temp_band"

# CSV column -> model feature name.
CSV_FEATURE_COLUMNS = {"temperature_C": "temperature", "humidity_%": "humidity"}


def load_dataset(path: str) -> pd.DataFrame:
    """Load the marine buoy CSV and reduce it to features + label."""
    df = pd.read_csv(path)

    frame = df.rename(columns=CSV_FEATURE_COLUMNS)[[*FEATURES, TARGET]]
    frame[FEATURES] = frame[FEATURES].apply(pd.to_numeric, errors="coerce")
    frame = frame.dropna(subset=[*FEATURES, TARGET])
    return frame


def build_pipeline() -> Pipeline:
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                RandomForestClassifier(
                    n_estimators=300,
                    max_depth=12,
                    min_samples_leaf=5,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the MaaS sea-temperature model.")
    parser.add_argument(
        "--dataset",
        default=os.environ.get("DATASET_PATH", "data/marine_buoy_readings.csv"),
        help="Path to the marine buoy CSV dataset.",
    )
    parser.add_argument(
        "--out-dir",
        default=os.environ.get("MODEL_DIR", "model"),
        help="Directory where model.joblib and metrics.json are written.",
    )
    args = parser.parse_args()

    frame = load_dataset(args.dataset)
    X = frame[FEATURES]
    y = frame[TARGET]

    # 70% train / 15% validation / 15% test, stratified so every class is present
    # in each split. Validation is used to sanity-check before touching test.
    X_train, X_tmp, y_train, y_tmp = train_test_split(
        X, y, test_size=0.30, random_state=42, stratify=y
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_tmp, y_tmp, test_size=0.50, random_state=42, stratify=y_tmp
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    def evaluate(features: pd.DataFrame, labels: pd.Series) -> dict:
        preds = pipeline.predict(features)
        return {
            "accuracy": round(float(accuracy_score(labels, preds)), 4),
            "macro_f1": round(float(f1_score(labels, preds, average="macro")), 4),
        }

    classes = sorted(y.unique().tolist())
    test_preds = pipeline.predict(X_test)

    metrics = {
        "model_type": "RandomForestClassifier",
        "task": "classification",
        "target": TARGET,
        "features": FEATURES,
        "classes": classes,
        "dataset": os.path.basename(args.dataset),
        "samples": {
            "total": int(len(frame)),
            "train": int(len(X_train)),
            "validation": int(len(X_val)),
            "test": int(len(X_test)),
        },
        "validation": evaluate(X_val, y_val),
        "test": evaluate(X_test, y_test),
        "test_confusion_matrix": {
            "labels": classes,
            "matrix": confusion_matrix(y_test, test_preds, labels=classes).tolist(),
        },
        "feature_importances": {
            feat: round(float(imp), 4)
            for feat, imp in zip(FEATURES, pipeline.named_steps["clf"].feature_importances_)
        },
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "note": (
            "Real hourly observations from Irish Marine Institute weather buoys. Air "
            "temperature + relative humidity predict the sea-temperature band (cold/mild/"
            "warm) with genuine signal, demonstrating a complete train -> serve -> consume "
            "MLOps pipeline over the live sensor stream."
        ),
    }

    os.makedirs(args.out_dir, exist_ok=True)
    joblib.dump(pipeline, os.path.join(args.out_dir, "model.joblib"))
    with open(os.path.join(args.out_dir, "metrics.json"), "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)

    print("Model trained and saved to", args.out_dir)
    print(json.dumps({"validation": metrics["validation"], "test": metrics["test"]}, indent=2))
    print("\nTest classification report:\n")
    print(classification_report(y_test, test_preds, labels=classes, zero_division=0))


if __name__ == "__main__":
    main()
