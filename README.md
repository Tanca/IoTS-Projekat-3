# IoT Project 3 — eKuiper CEP + Model-as-a-Service Analytics

An event-driven IoT analytics pipeline. The **Analytics** microservice no longer
computes analytics on its own — instead it analyses the MQTT sensor stream using
**two dedicated services**:

1. **eKuiper** — a streaming / Complex Event Processing (CEP) engine that detects
   events of interest and publishes them back onto MQTT.
2. **MaaS (Model as a Service)** — a Python/FastAPI microservice serving a
   scikit-learn machine-learning model over REST.

Everything runs as **Docker containers**, and a **web dashboard** visualises the
whole pipeline live.

> This is a self-contained repository. The `ingestion-service`, `storage-service`,
> the shared `event-generator`, the sample dataset, the Mosquitto config and the
> Postgres schema were carried over from an earlier MQTT project — only the pieces
> this project actually needs. Everything lives inside this folder, so it can be
> opened and pushed to Git on its own.

## Architecture

```
                       ┌──────────────────────────────────────────────┐
   CSV dataset         │                MQTT broker                    │
       │               │               (Mosquitto)                     │
       ▼               │                                               │
 ingestion-service ───▶│ iot/readings ─┬─────────────▶ storage-service │──▶ PostgreSQL
                       │               │                               │
                       │               ├──▶ eKuiper (CEP rules)        │
                       │               │        │                      │
                       │      iot/events ◀───────┘                      │
                       │          │                                     │
                       │          ▼                                     │
                       │   analytics-service ──REST──▶ maas-service (ML)│
                       │          │                                     │
                       │   iot/analytics                                │
                       └──────────┼───────────────────────────────────┘
                                  ▼
                          Web dashboard (:8080)
```

## Microservices

| Service | Tech | Role | Ports |
| --- | --- | --- | --- |
| `mosquitto` | Eclipse Mosquitto 2 | MQTT broker | `1883` |
| `postgres` | PostgreSQL 16 | Sensor-reading storage | `5433→5432` |
| `ingestion-service` | Node.js *(reused)* | Publishes `SensorReading` events to `iot/readings` | — |
| `storage-service` | Node.js *(reused)* | Persists readings to PostgreSQL | — |
| **`ekuiper`** | eKuiper (LF Edge) | **CEP**: subscribes to `iot/readings`, applies rules, publishes detections to `iot/events` | `9081` |
| `ekuiper-setup` | curl (one-shot) | Registers the eKuiper stream + rules via REST | — |
| **`maas-service`** | **Python + FastAPI + scikit-learn** | **MaaS**: serves the sea-temperature ML model over REST | `8000` |
| **`analytics-service`** | **Node.js + Express + WS** | **Upgraded Analytics**: consumes CEP events, calls MaaS, serves dashboard | `8080` |

Per-service documentation:

- [`maas-service/README.md`](./maas-service/README.md)
- [`ekuiper/README.md`](./ekuiper/README.md)
- [`analytics-service/README.md`](./analytics-service/README.md)

## How the requirements map to the code

| PDF requirement | Where |
| --- | --- |
| 1a. Analytics uses eKuiper CEP over MQTT | `analytics-service` subscribes to `iot/events` |
| 1b. Analytics uses MaaS REST endpoints | `analytics-service` → `maas-service` `/predict` |
| 2. eKuiper on same topic, rules → new topic | `ekuiper` + `ekuiper/setup-rules.sh` |
| 3. MaaS with Python/FastAPI + ML model | `maas-service` (RandomForest, scikit-learn) |
| 4. Docker containers + web app | `docker-compose.yml` + dashboard at `:8080` |
| 5. Source on GitHub with description | this README |

## Quick start

From this folder:

```bash
# Build and start the whole stack
docker compose up -d --build

# Register the eKuiper CEP rules (also runs automatically as ekuiper-setup)
docker compose logs ekuiper-setup

# Open the dashboard and click "Start live stream" to replay the buoy data
# continuously (readings + CEP events + ML predictions update in real time):
#   http://localhost:8080

# ...or publish a one-off batch from the CLI instead (300 events at 20/s):
docker compose run --rm ingestion-service
```

Useful checks:

```bash
# MaaS prediction
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" -d '{"temperature":34,"humidity":42}'

# MaaS model card / metrics
curl http://localhost:8000/model/info

# eKuiper rules
curl http://localhost:9081/rules

# Watch CEP detections on the bus
docker compose exec mosquitto mosquitto_sub -t iot/events -v

# Analytics summary
curl http://localhost:8080/api/summary
```

Tear down:

```bash
docker compose down -v
```

## Notes

- The bundled `marine_buoy_readings.csv` holds real hourly observations from Irish
  Marine Institute weather buoys. The model predicts the sea-temperature band
  (cold/mild/warm) from air temperature + humidity at ~78% test accuracy (vs. 33%
  chance) — genuine signal, reported transparently at `/model/info`. The dataset
  feeds both the model training and the live MQTT stream generator.
- **Dataset note:** In Project 2's Smart-Farming dataset, temperature and humidity
  have essentially no statistical relationship to the target (near-identical class
  means, correlations ≈ 0), so the model sat at chance (~25%). That's why this
  project uses real marine-buoy measurements instead, where the features carry
  genuine signal (78%). The architecture (stream → CEP → MaaS → dashboard) is
  unchanged — only the data source differs.
- The stack uses its own container names (`p3-*`), volumes (`p3_*`) and the
  Postgres host port `5433`. Only one MQTT stack should bind `1883` at a time.
