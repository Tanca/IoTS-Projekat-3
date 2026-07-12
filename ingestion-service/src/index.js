const fs = require("node:fs");
const path = require("node:path");
const mqtt = require("mqtt");
const { EventGenerator, loadCsvDataset } = require("../../shared/event-generator");

const DEFAULT_DATASET_PATHS = [
  path.resolve(__dirname, "../../shared/sample-data/sensor-data.csv"),
  path.resolve(__dirname, "../../shared/sample-data/marine_buoy_readings.csv")
];

function readIntegerEnv(name, fallback, { min = 0 } = {}) {
  const value = process.env[name];

  if (value === undefined || value === "") {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer greater than or equal to ${min}.`);
  }

  return parsed;
}

function readQos() {
  const qos = readIntegerEnv("MQTT_QOS", 0, { min: 0 });

  if (![0, 1, 2].includes(qos)) {
    throw new Error("MQTT_QOS must be 0, 1, or 2.");
  }

  return qos;
}

function resolveDatasetPath() {
  if (process.env.DATASET_PATH) {
    return path.resolve(process.env.DATASET_PATH);
  }

  const datasetPath = DEFAULT_DATASET_PATHS.find((candidate) => fs.existsSync(candidate));

  if (!datasetPath) {
    throw new Error(`No dataset found. Checked: ${DEFAULT_DATASET_PATHS.join(", ")}`);
  }

  return datasetPath;
}

function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function connectMqtt(mqttUrl) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(mqttUrl, {
      clientId: `mqtt-ingestion-${process.pid}`,
      clean: false,  // Persistent session for QoS 1/2 recovery
      reconnectPeriod: 1000,
      connectTimeout: 30000
    });

    client.once("connect", () => resolve(client));
    client.once("error", reject);
  });
}

function publishEvent(client, topic, event, qos) {
  return new Promise((resolve, reject) => {
    client.publish(topic, JSON.stringify(event), { qos }, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function closeMqtt(client) {
  return new Promise((resolve) => {
    client.end(false, {}, resolve);
  });
}

async function main() {
  const mqttUrl = process.env.MQTT_URL || "mqtt://mosquitto:1883";
  const topic = process.env.MQTT_TOPIC || "iot/readings";
  const qos = readQos();
  const totalMessages = readIntegerEnv("TOTAL_MESSAGES", 100, { min: 0 });
  const messagesPerSecond = readIntegerEnv("MESSAGES_PER_SECOND", 10, { min: 1 });
  const deviceCount = readIntegerEnv("DEVICE_COUNT", 10, { min: 1 });
  const datasetPath = resolveDatasetPath();

  const records = loadCsvDataset(datasetPath);
  const generator = new EventGenerator(records);
  const events = generator.generateEvents({
    totalMessages,
    deviceCount
  });
  const intervalMs = 1000 / messagesPerSecond;
  const startedAt = Date.now();
  let publishedMessages = 0;

  console.log("MQTT ingestion service starting", {
    mqttUrl,
    topic,
    qos,
    totalMessages,
    messagesPerSecond,
    deviceCount,
    datasetPath,
    normalizedRecords: records.length
  });

  const client = await connectMqtt(mqttUrl);

  try {
    for (const event of events) {
      const publishStartedAt = Date.now();
      await publishEvent(client, topic, event, qos);
      publishedMessages += 1;

      const elapsed = Date.now() - publishStartedAt;
      const remainingDelay = intervalMs - elapsed;

      if (remainingDelay > 0 && publishedMessages < totalMessages) {
        await delay(remainingDelay);
      }
    }
  } finally {
    await closeMqtt(client);
  }

  const durationMs = Date.now() - startedAt;
  const durationSeconds = durationMs / 1000;
  const approximateMessagesPerSecond = durationSeconds > 0
    ? publishedMessages / durationSeconds
    : publishedMessages;

  console.log("MQTT ingestion run summary", {
    totalAttemptedMessages: totalMessages,
    totalPublishedMessages: publishedMessages,
    qos,
    durationMs,
    approximateMessagesPerSecond: Number(approximateMessagesPerSecond.toFixed(2))
  });
}

main().catch((error) => {
  console.error("MQTT ingestion service failed", error);
  process.exitCode = 1;
});
