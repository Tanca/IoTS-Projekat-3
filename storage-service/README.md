# MQTT Storage Service

Subscribes to `iot/readings`, parses SensorReading JSON messages, and stores them in PostgreSQL using batch inserts.

## Environment Variables

| Variable | Default |
| --- | --- |
| `MQTT_URL` | `mqtt://mosquitto:1883` |
| `MQTT_TOPIC` | `iot/readings` |
| `MQTT_QOS` | `0` |
| `POSTGRES_HOST` | `postgres` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `iotdb` |
| `POSTGRES_USER` | `iotuser` |
| `POSTGRES_PASSWORD` | `iotpass` |

Messages are inserted with `broker_type = "mqtt"` and `delivery_mode = "qos-0"`, `"qos-1"`, or `"qos-2"` based on the received MQTT packet QoS.

The service code defaults `MQTT_QOS` to `0` when run directly. Docker Compose sets the subscriber QoS to `2` by default so verification runs can publish QoS 0, 1, and 2 messages without recreating the service each time.

The service flushes batches of 500 messages and flushes any remaining buffered messages during graceful shutdown.
