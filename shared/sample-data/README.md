# Sample Data

## `marine_buoy_readings.csv`

Real hourly weather observations from **Irish Marine Institute** weather buoys
(stations M1–M6, M4-Archive, Belmullet-AMETS). A balanced 39,999-row sample was
prepared from the raw export: rows missing air temperature, humidity, or sea
temperature were dropped, and the sea temperature was binned into three
equal-frequency bands (the ML target).

This one file feeds **both** halves of the pipeline:

- the **stream generator / ingestion service** reads `sensor_id`, `timestamp`,
  `temperature_C`, `humidity_%` to publish `SensorReading` events, and
- the **MaaS model trainer** uses `temperature_C` + `humidity_%` as features and
  `sea_temp_band` as the classification label.

### Columns

| Column | Meaning | Used by |
| --- | --- | --- |
| `sensor_id` | Buoy station id → `deviceId` | stream generator |
| `timestamp` | ISO-8601 observation time → `createdAt` | stream generator |
| `temperature_C` | Air temperature (°C) → `temperature` | stream + model feature |
| `humidity_%` | Relative humidity (%) → `humidity` | stream + model feature |
| `sea_temperature_C` | Sea surface temperature (°C) | source of the label |
| `sea_temp_band` | `cold` / `mild` / `warm` (tertiles of sea temp) | model **target** |

Band edges: `cold` ≤ 11.1 °C, `mild` 11.1–13.8 °C, `warm` > 13.8 °C.

### Field mapping to the shared event contract

| Contract field | Source column |
| --- | --- |
| `messageId` | generated UUID |
| `deviceId` | `sensor_id` |
| `temperature` | `temperature_C` |
| `humidity` | `humidity_%` |
| `createdAt` | `timestamp` |
