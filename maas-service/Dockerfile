FROM python:3.12-slim

WORKDIR /app

# Install dependencies first so they stay cached across code changes.
COPY maas-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Application code + training script.
COPY maas-service/app ./app
COPY maas-service/train_model.py ./train_model.py

# The dataset is copied into the image so the model is trained reproducibly
# at build time (train -> serve in one image).
COPY shared/sample-data/marine_buoy_readings.csv ./data/marine_buoy_readings.csv

RUN python train_model.py --dataset data/marine_buoy_readings.csv --out-dir model

EXPOSE 8000
# Two workers so concurrent prediction bursts from the Analytics service run in
# parallel across the Python GIL (each worker loads its own copy of the model).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
