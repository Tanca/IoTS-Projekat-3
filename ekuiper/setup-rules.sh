#!/bin/sh
# Registers the MQTT source stream and the CEP rules in eKuiper via its REST API.
#
# eKuiper subscribes to the SAME MQTT topic as the Analytics service (iot/readings),
# applies the rules below to detect "events of interest", and republishes each
# detection to a NEW MQTT topic (iot/events) that the Analytics service consumes.
#
# The script is idempotent: it drops any existing stream/rules first, then recreates
# them, so it can be re-run safely.

set -e

EKUIPER_URL="${EKUIPER_URL:-http://ekuiper:9081}"
MQTT_SERVER="${MQTT_SERVER:-tcp://mosquitto:1883}"
SOURCE_TOPIC="${SOURCE_TOPIC:-iot/readings}"
EVENTS_TOPIC="${EVENTS_TOPIC:-iot/events}"

echo "[ekuiper-setup] Waiting for eKuiper REST API at ${EKUIPER_URL} ..."
until curl -sf "${EKUIPER_URL}/streams" >/dev/null 2>&1; do
  sleep 2
done
echo "[ekuiper-setup] eKuiper is up."

# ---------------------------------------------------------------------------
# Source stream: reads SensorReading JSON events from the MQTT broker.
# ---------------------------------------------------------------------------
curl -s -X DELETE "${EKUIPER_URL}/rules/cold_air"          >/dev/null 2>&1 || true
curl -s -X DELETE "${EKUIPER_URL}/rules/warm_spell"        >/dev/null 2>&1 || true
curl -s -X DELETE "${EKUIPER_URL}/rules/high_humidity"     >/dev/null 2>&1 || true
curl -s -X DELETE "${EKUIPER_URL}/rules/cold_damp"         >/dev/null 2>&1 || true
curl -s -X DELETE "${EKUIPER_URL}/rules/window_cold_avg"   >/dev/null 2>&1 || true
curl -s -X DELETE "${EKUIPER_URL}/streams/iot_readings"    >/dev/null 2>&1 || true

echo "[ekuiper-setup] Creating source stream iot_readings ..."
curl -s -X POST "${EKUIPER_URL}/streams" \
  -H "Content-Type: application/json" \
  -d "{\"sql\": \"CREATE STREAM iot_readings (messageId string, deviceId string, temperature float, humidity float, createdAt string) WITH (DATASOURCE=\\\"${SOURCE_TOPIC}\\\", FORMAT=\\\"json\\\", TYPE=\\\"mqtt\\\", SHARED=\\\"true\\\")\"}"
echo

create_rule() {
  RULE_ID="$1"
  RULE_SQL="$2"
  echo "[ekuiper-setup] Creating rule ${RULE_ID} ..."
  curl -s -X POST "${EKUIPER_URL}/rules" \
    -H "Content-Type: application/json" \
    -d "{
          \"id\": \"${RULE_ID}\",
          \"sql\": \"${RULE_SQL}\",
          \"actions\": [
            { \"mqtt\": { \"server\": \"${MQTT_SERVER}\", \"topic\": \"${EVENTS_TOPIC}\", \"sendSingle\": true } },
            { \"log\": {} }
          ]
        }"
  echo
}

# ---------------------------------------------------------------------------
# CEP rules for the marine buoy stream (air temperature 1-22 C, humidity 27-100 %).
# Each detection is enriched with an eventType + description so the Analytics
# service can categorise it. Rule 4 is a real streaming/CEP window.
# ---------------------------------------------------------------------------

# 1) Per-event: cold air (below 8 C).
create_rule "cold_air" \
  "SELECT deviceId, temperature, humidity, createdAt, \\\"COLD_AIR\\\" AS eventType, \\\"Air temperature below 8C\\\" AS description FROM iot_readings WHERE temperature < 8"

# 1b) Per-event: warm spell (above 15.3 C) - a warm Irish summer's day.
create_rule "warm_spell" \
  "SELECT deviceId, temperature, humidity, createdAt, \\\"WARM_SPELL\\\" AS eventType, \\\"Air temperature above 15.3C (warm spell)\\\" AS description FROM iot_readings WHERE temperature > 15.3"

# 2) Per-event: near-saturated air (humidity above 93 %) - fog / condensation risk.
create_rule "high_humidity" \
  "SELECT deviceId, temperature, humidity, createdAt, \\\"HIGH_HUMIDITY\\\" AS eventType, \\\"Humidity above 93pct - fog/saturation risk\\\" AS description FROM iot_readings WHERE humidity > 93"

# 3) Per-event CEP: combined cold-and-damp condition (cold AND saturated).
create_rule "cold_damp" \
  "SELECT deviceId, temperature, humidity, createdAt, \\\"COLD_DAMP\\\" AS eventType, \\\"Cold and saturated: temp<9 and humidity>90\\\" AS description FROM iot_readings WHERE temperature < 9 AND humidity > 90"

# 4) Streaming CEP: 10-second tumbling window whose average temperature is below 11 C.
# Aggregates are aliased to distinct names (avgTemperature/avgHumidity) so they do
# not shadow the source columns referenced in HAVING.
create_rule "window_cold_avg" \
  "SELECT deviceId, AVG(temperature) AS avgTemperature, AVG(humidity) AS avgHumidity, COUNT(*) AS samples, \\\"WINDOW_COLD_AVG\\\" AS eventType, \\\"10s window avg air temperature below 11C\\\" AS description FROM iot_readings GROUP BY deviceId, TUMBLINGWINDOW(ss, 10) HAVING AVG(temperature) < 11"

echo "[ekuiper-setup] Done. Registered streams:"
curl -s "${EKUIPER_URL}/streams"; echo
echo "[ekuiper-setup] Registered rules:"
curl -s "${EKUIPER_URL}/rules"; echo
