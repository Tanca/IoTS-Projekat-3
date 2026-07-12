# MQTT Ingestion Service

Generates standardized SensorReading events from the shared CSV dataset and publishes them to Mosquitto on `iot/readings`.

This service exits after publishing `TOTAL_MESSAGES`.

## Environment Variables

| Variable | Default |
| --- | --- |
| `MQTT_URL` | `mqtt://mosquitto:1883` |
| `MQTT_TOPIC` | `iot/readings` |
| `MQTT_QOS` | `0` |
| `TOTAL_MESSAGES` | `100` |
| `MESSAGES_PER_SECOND` | `10` |
| `DEVICE_COUNT` | `10` |
| `DATASET_PATH` | auto-detect sample dataset |

`MQTT_QOS` supports `0`, `1`, and `2`.

## Local Docker Run

```bash
docker compose run --rm mqtt-ingestion-service
```

With QoS 1:

```bash
docker compose run --rm -e MQTT_QOS=1 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
```

With QoS 2:

```bash
docker compose run --rm -e MQTT_QOS=2 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
```

On Windows PowerShell:

```powershell
docker compose run --rm -e MQTT_QOS=1 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
```
