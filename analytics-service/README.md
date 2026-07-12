# Analytics Microservice (Project 3, upgraded)

Node.js microservice that upgrades the Project 2 Analytics service to analyse the
sensor stream using **both** required mechanisms (requirement 1):

- **(a) eKuiper CEP** — it subscribes to `iot/events`, the MQTT topic that the
  eKuiper engine publishes detected events to.
- **(b) MaaS ML** — for every reading and every CEP event it calls the MaaS REST
  API (`POST /predict`) to attach a sea-temperature-band prediction.

It also keeps the original **10-second tumbling-window** temperature analytics,
serves a live **web dashboard** (with **Start/Stop live-stream buttons** that
replay real buoy readings onto MQTT so the whole pipeline runs continuously),
exposes a **REST + WebSocket API**, and republishes the enriched (CEP + ML)
events to the `iot/analytics` MQTT topic.

## Data flow

```
ingestion ─▶ iot/readings ─▶ eKuiper (CEP rules) ─▶ iot/events ─▶ Analytics ─▶ MaaS /predict
                     └────────────────────────────────────────────▶ Analytics (raw + window)
                                                                          │
                                                        REST + WebSocket ─┴─▶ web dashboard
                                                                          └─▶ iot/analytics
```

## REST / WebSocket API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/` | Web dashboard (static). |
| `GET` | `/api/health` | Liveness + MaaS availability. |
| `GET` | `/api/summary` | Totals, counts, last window, config. |
| `GET` | `/api/readings` | Recent readings + their ML predictions. |
| `GET` | `/api/events` | Recent eKuiper CEP events + ML predictions. |
| `GET` | `/api/windows` | Tumbling-window history. |
| `GET` | `/api/model` | Proxy to MaaS `/model/info` (model card). |
| `POST` | `/api/stream/start` | Start the live replay stream (dashboard **Start** button). |
| `POST` | `/api/stream/stop` | Stop the live replay stream (dashboard **Stop** button). |
| `WS`  | `/ws` | Live push of readings / events / windows / stream state. |

## Configuration (env)

| Variable | Default | Purpose |
| --- | --- | --- |
| `MQTT_URL` | `mqtt://mosquitto:1883` | MQTT broker. |
| `READINGS_TOPIC` | `iot/readings` | Raw sensor stream. |
| `EVENTS_TOPIC` | `iot/events` | eKuiper CEP detections. |
| `ANALYTICS_TOPIC` | `iot/analytics` | Enriched output. |
| `MAAS_URL` | `http://maas-service:8000` | MaaS REST base URL. |
| `HTTP_PORT` | `8080` | Dashboard/API port. |
| `WINDOW_SECONDS` | `10` | Tumbling window size. |
| `ALERT_THRESHOLD` | `11` | Cold-average alert: flag windows below this avg air temp (°C). |
| `MAAS_MAX_INFLIGHT` | `16` | Max concurrent MaaS prediction calls. |
| `STREAM_RATE` | `5` | Live-stream replay rate (readings/sec) when Start is clicked. |

Open the dashboard at <http://localhost:8080>.
