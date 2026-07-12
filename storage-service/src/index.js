const { randomUUID } = require("node:crypto");
const mqtt = require("mqtt");
const { Pool } = require("pg");

const BATCH_SIZE = 500;

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

function normalizeEvent(payload) {
  const event = JSON.parse(payload.toString("utf8"));
  const temperature = Number(event.temperature);
  const humidity = Number(event.humidity);
  const createdAt = new Date(event.createdAt);

  if (
    !event.messageId ||
    !event.deviceId ||
    !Number.isFinite(temperature) ||
    !Number.isFinite(humidity) ||
    Number.isNaN(createdAt.getTime())
  ) {
    return null;
  }

  return {
    messageId: event.messageId,
    deviceId: event.deviceId,
    temperature,
    humidity,
    createdAt: createdAt.toISOString()
  };
}

function buildInsert(records) {
  const columnsPerRecord = 9;
  const values = [];
  const placeholders = records.map((record, recordIndex) => {
    const offset = recordIndex * columnsPerRecord;
    values.push(
      randomUUID(),
      record.messageId,
      record.deviceId,
      record.temperature,
      record.humidity,
      record.createdAt,
      "mqtt",
      record.deliveryMode,
      new Date()
    );

    return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9})`;
  });

  return {
    text: `
      INSERT INTO sensor_readings (
        id,
        message_id,
        device_id,
        temperature,
        humidity,
        created_at,
        broker_type,
        delivery_mode,
        received_at
      )
      VALUES ${placeholders.join(", ")}
    `,
    values
  };
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
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || "postgres",
    port: readIntegerEnv("POSTGRES_PORT", 5432, { min: 1 }),
    database: process.env.POSTGRES_DB || "iotdb",
    user: process.env.POSTGRES_USER || "iotuser",
    password: process.env.POSTGRES_PASSWORD || "iotpass"
  });
  const client = mqtt.connect(mqttUrl, {
    clientId: "mqtt-storage-service",  // Stable ID for persistent session
    clean: false,  // Persistent session - broker queues messages during disconnect
    reconnectPeriod: 1000,
    connectTimeout: 30000
  });
  let buffer = [];
  let flushing = false;
  let flushAgain = false;
  let consumedMessages = 0;
  let insertedMessages = 0;
  let skippedMessages = 0;
  let shuttingDown = false;

  async function flushBatch() {
    if (flushing) {
      flushAgain = true;
      return;
    }

    if (buffer.length === 0) {
      return;
    }

    flushing = true;
    const batch = buffer;
    buffer = [];

    try {
      const insert = buildInsert(batch);
      await pool.query(insert.text, insert.values);
      insertedMessages += batch.length;
      console.log("MQTT storage batch inserted", {
        batchSize: batch.length,
        insertedMessages
      });
    } catch (error) {
      buffer = batch.concat(buffer);
      console.error("MQTT storage batch insert failed", error);
    } finally {
      flushing = false;

      if (flushAgain) {
        flushAgain = false;
        await flushBatch();
      }
    }
  }

  const flushInterval = setInterval(() => {
    flushBatch().catch((error) => {
      console.error("MQTT storage periodic flush failed", error);
    });
  }, 1000);

  client.on("connect", () => {
    client.subscribe(topic, { qos }, (error) => {
      if (error) {
        console.error("MQTT storage subscribe failed", error);
        return;
      }

      console.log("MQTT storage service subscribed", {
        mqttUrl,
        topic,
        qos,
        batchSize: BATCH_SIZE
      });
    });
  });

  client.on("message", (_topic, payload, packet) => {
    if (shuttingDown) {
      return;
    }

    consumedMessages += 1;

    try {
      const event = normalizeEvent(payload);

      if (!event) {
        skippedMessages += 1;
        return;
      }

      buffer.push({
        ...event,
        deliveryMode: `qos-${packet.qos}`
      });

      if (buffer.length >= BATCH_SIZE) {
        flushBatch().catch((error) => {
          console.error("MQTT storage threshold flush failed", error);
        });
      }
    } catch (error) {
      skippedMessages += 1;
      console.error("MQTT storage skipped malformed message", {
        error: error.message
      });
    }
  });

  client.on("error", (error) => {
    console.error("MQTT storage client error", error);
  });

  async function shutdown(signal) {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log("MQTT storage service shutting down", { signal });
    clearInterval(flushInterval);
    await closeMqtt(client);
    await flushBatch();
    await pool.end();
    console.log("MQTT storage service stopped", {
      consumedMessages,
      insertedMessages,
      skippedMessages,
      remainingBufferedMessages: buffer.length
    });
  }

  process.on("SIGINT", () => {
    shutdown("SIGINT").then(() => process.exit(0));
  });

  process.on("SIGTERM", () => {
    shutdown("SIGTERM").then(() => process.exit(0));
  });
}

main().catch((error) => {
  console.error("MQTT storage service failed", error);
  process.exitCode = 1;
});
