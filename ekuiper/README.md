# eKuiper CEP Layer

[eKuiper](https://ekuiper.org/) is an edge stream-processing / Complex Event
Processing (CEP) engine. In Project 3 it plays the role described in requirement 2:

- It **subscribes to the same MQTT topic as Analytics** — `iot/readings` — via an
  MQTT source stream (`iot_readings`).
- It applies **SQL-defined CEP rules** to detect events of interest.
- It **publishes each detection to a new MQTT topic** — `iot/events` — which the
  Analytics microservice consumes.

## Configuration

The `ekuiper` container runs the stock `lfedge/ekuiper` image. Its default MQTT
source server is pointed at the broker through the environment variable
`MQTT_SOURCE__DEFAULT__SERVER=tcp://mosquitto:1883` (set in `docker-compose.yml`).

The one-shot `ekuiper-setup` container runs [`setup-rules.sh`](./setup-rules.sh),
which waits for the eKuiper REST API (port `9081`) and then registers the source
stream and the rules below. It is idempotent and can be re-run.

## Rules

The marine buoy stream carries air temperature (1–22 °C) and humidity (27–100 %),
so the rules detect cold / saturated (fog-risk) conditions:

| Rule | Type | Condition | Emitted `eventType` |
| --- | --- | --- | --- |
| `cold_air` | per-event | `temperature < 8` | `COLD_AIR` |
| `warm_spell` | per-event | `temperature > 15.3` | `WARM_SPELL` |
| `high_humidity` | per-event | `humidity > 93` | `HIGH_HUMIDITY` |
| `cold_damp` | per-event CEP | `temperature < 9 AND humidity > 90` | `COLD_DAMP` |
| `window_cold_avg` | streaming CEP | 10s tumbling window, `AVG(temperature) < 11` | `WINDOW_COLD_AVG` |

Every rule sinks to `iot/events` (MQTT) and also logs to the eKuiper log.

## Inspect at runtime

```bash
# List rules and their status
curl http://localhost:9081/rules
curl http://localhost:9081/rules/cold_damp/status

# Watch detected events on the MQTT bus
docker compose exec mosquitto \
  mosquitto_sub -t iot/events -v
```
