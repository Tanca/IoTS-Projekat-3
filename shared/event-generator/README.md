# Event Generator

Reusable dataset loader and SensorReading event generator for the IoT broker comparison project.

This module is intentionally independent from MQTT, Kafka, PostgreSQL, REST APIs, and Docker. It prepares standardized events that future ingestion services can publish through either broker.

## Dataset Structure

The current sample dataset is stored at:

```text
shared/sample-data/marine_buoy_readings.csv
```

The preview script also checks for this preferred future name first:

```text
shared/sample-data/sensor-data.csv
```

The actual dataset currently contains columns such as:

- `sensor_id`
- `timestamp`
- `temperature_C`
- `humidity_%`
- `sea_temperature_C`
- `sea_temp_band`

Only the fields needed for the shared SensorReading event
(`sensor_id`, `timestamp`, `temperature_C`, `humidity_%`) are used by this module;
the sea-temperature columns are consumed only by the MaaS model trainer.

## Field Mapping

| Dataset field | Event field |
| --- | --- |
| `sensor_id` | `deviceId` |
| `temperature_C` | `temperature` |
| `humidity_%` | `humidity` |
| `timestamp` | `createdAt` |
| generated with `crypto.randomUUID()` | `messageId` |

Rows are skipped when required fields are missing, temperature or humidity cannot be converted to numbers, or the timestamp cannot be normalized.

## Event Model

Every generated event follows this shape:

```json
{
  "messageId": "uuid",
  "deviceId": "string",
  "temperature": 22.5,
  "humidity": 60.1,
  "createdAt": "2024-03-19T00:00:00.000Z"
}
```

## Usage

Load normalized dataset records:

```js
const { loadCsvDataset } = require("./src");

const records = loadCsvDataset("../sample-data/marine_buoy_readings.csv");
```

Generate a finite set of events:

```js
const { EventGenerator } = require("./src");

const generator = new EventGenerator(records);
const events = generator.generateEvents({
  totalMessages: 100,
  deviceCount: 25
});
```

Create a streaming-style async generator:

```js
for await (const event of generator.streamEvents({ messagesPerSecond: 10 })) {
  console.log(event);
}
```

The stream is intentionally infinite. Future caller code should decide when to stop consuming it.

## Preview Command

From `shared/event-generator`:

```bash
npm run preview
```

Or run directly:

```bash
node scripts/preview-events.js --count 5
```

Optional controls:

```bash
node scripts/preview-events.js --count 5 --device-count 100
node scripts/preview-events.js --count 5 --dataset ../sample-data/marine_buoy_readings.csv
```

## Future MQTT and Kafka Reuse

Future MQTT and Kafka ingestion services should both import this module instead of duplicating dataset parsing logic.

The intended flow is:

1. Load normalized records with `loadCsvDataset`.
2. Create an `EventGenerator`.
3. Use `generateEvents` for finite benchmark runs or `streamEvents` for rate-controlled simulation.
4. Publish the generated SensorReading events through the chosen broker.

Broker-specific code should stay in the MQTT or Kafka service folders. Dataset loading and event creation should remain shared here.
