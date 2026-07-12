/**
 * Project 3 Analytics microservice.
 *
 * Upgrades the Project 2 Analytics service so that it analyses data using BOTH:
 *   (a) eKuiper CEP  - it subscribes to the `iot/events` topic that eKuiper
 *       publishes detected events to (requirement 1a / 2), and
 *   (b) the MaaS ML microservice - it calls the MaaS REST API to classify each
 *       sensor reading (requirement 1b / 3).
 *
 * It also keeps the original tumbling-window temperature analytics, exposes a
 * REST + WebSocket API, serves the web dashboard, and republishes an enriched
 * analytics summary to the `iot/analytics` MQTT topic.
 */

const http = require("node:http");
const path = require("node:path");
const express = require("express");
const mqtt = require("mqtt");
const { WebSocketServer } = require("ws");
const { loadCsvDataset, createSensorReading } = require("../../shared/event-generator");

function readIntEnv(name, fallback, { min = 0 } = {}) {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer >= ${min}.`);
  }
  return parsed;
}

function readNumberEnv(name, fallback) {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`${name} must be a number.`);
  return parsed;
}

const config = {
  mqttUrl: process.env.MQTT_URL || "mqtt://mosquitto:1883",
  readingsTopic: process.env.READINGS_TOPIC || "iot/readings",
  eventsTopic: process.env.EVENTS_TOPIC || "iot/events",
  analyticsTopic: process.env.ANALYTICS_TOPIC || "iot/analytics",
  maasUrl: process.env.MAAS_URL || "http://maas-service:8000",
  httpPort: readIntEnv("HTTP_PORT", 8080, { min: 1 }),
  windowSeconds: readIntEnv("WINDOW_SECONDS", 10, { min: 1 }),
  alertThreshold: readNumberEnv("ALERT_THRESHOLD", 11),
  maxItems: readIntEnv("MAX_ITEMS", 60, { min: 1 }),
  // Cap concurrent MaaS calls so a burst of readings can't overwhelm the model
  // service and trip timeouts. MaaS serves a prediction in ~20ms, so 16 in-flight
  // comfortably absorbs the default ingestion rate without skipping readings,
  // while still bounding memory under an extreme burst.
  maxInflight: readIntEnv("MAAS_MAX_INFLIGHT", 16, { min: 1 }),
  qos: readIntEnv("MQTT_QOS", 0, { min: 0 }),
  // Dashboard-driven "live stream": replay real buoy readings onto MQTT at this
  // rate (readings/sec) when the user clicks Start on the dashboard.
  streamRate: readIntEnv("STREAM_RATE", 5, { min: 1 }),
  datasetPath:
    process.env.DATASET_PATH ||
    path.resolve(__dirname, "..", "..", "shared", "sample-data", "marine_buoy_readings.csv")
};

// ---------------------------------------------------------------------------
// In-memory analytics state (served over REST/WebSocket to the dashboard).
// ---------------------------------------------------------------------------
const state = {
  startedAt: new Date().toISOString(),
  streaming: false,
  totalReadings: 0,
  totalEvents: 0,
  totalPredictions: 0,
  predictionErrors: 0,
  predictionsSkipped: 0,
  maasAvailable: false,
  eventCounts: {},
  mlClassCounts: {},
  recentReadings: [],
  recentEvents: [],
  windows: [],
  lastWindow: null
};

// Tumbling-window accumulators.
let windowStart = Date.now();
let windowCount = 0;
let windowTempSum = 0;
let windowHumSum = 0;

function pushCapped(list, item) {
  list.unshift(item);
  if (list.length > config.maxItems) list.length = config.maxItems;
}

// ---------------------------------------------------------------------------
// MaaS REST client.
// ---------------------------------------------------------------------------
async function callMaas(pathname, options = {}, timeoutMs = 4000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${config.maasUrl}${pathname}`, {
      ...options,
      signal: controller.signal
    });
    if (!response.ok) throw new Error(`MaaS ${pathname} -> HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

let inflight = 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Ask MaaS to classify a reading. Concurrency is capped at config.maxInflight.
 * For high-volume raw readings we pass skipIfBusy=true so a burst is sampled
 * rather than queued (dropped calls count as "skipped", not "error"). CEP events
 * wait for a free slot so they always get a prediction. Returns null on skip/failure.
 */
async function predict(temperature, humidity, { skipIfBusy = false } = {}) {
  if (skipIfBusy && inflight >= config.maxInflight) {
    state.predictionsSkipped += 1;
    return null;
  }
  while (inflight >= config.maxInflight) {
    await sleep(20);
  }

  inflight += 1;
  try {
    const result = await callMaas("/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ temperature, humidity })
    });
    state.totalPredictions += 1;
    state.maasAvailable = true;
    state.mlClassCounts[result.predicted_class] =
      (state.mlClassCounts[result.predicted_class] || 0) + 1;
    return result;
  } catch (error) {
    state.predictionErrors += 1;
    state.maasAvailable = false;
    return null;
  } finally {
    inflight -= 1;
  }
}

// ---------------------------------------------------------------------------
// MQTT wiring.
// ---------------------------------------------------------------------------
const client = mqtt.connect(config.mqttUrl, {
  clientId: "analytics-service-v3",
  clean: false,
  reconnectPeriod: 1000,
  connectTimeout: 30000
});

client.on("connect", () => {
  client.subscribe([config.readingsTopic, config.eventsTopic], { qos: config.qos }, (error) => {
    if (error) {
      console.error("Analytics subscribe failed", error);
      return;
    }
    console.log("Analytics v3 subscribed", {
      readingsTopic: config.readingsTopic,
      eventsTopic: config.eventsTopic,
      maasUrl: config.maasUrl
    });
  });
});

client.on("message", (topic, payload) => {
  let data;
  try {
    data = JSON.parse(payload.toString("utf8"));
  } catch (error) {
    console.error("Analytics skipped malformed message", { topic, error: error.message });
    return;
  }

  if (topic === config.readingsTopic) {
    handleReading(data).catch((error) => {
      console.error("handleReading failed", error.message);
    });
  } else if (topic === config.eventsTopic) {
    handleCepEvent(data).catch((error) => {
      console.error("handleCepEvent failed", error.message);
    });
  }
});

client.on("error", (error) => console.error("Analytics MQTT error", error.message));

// ---------------------------------------------------------------------------
// Dashboard-driven live stream. Replays real buoy readings from the CSV onto
// the readings topic so the whole pipeline (CEP + storage + MaaS + dashboard)
// runs continuously. Start/Stop is controlled from the web app.
// ---------------------------------------------------------------------------
let replayRecords = null;
let streamTimer = null;
let streamIndex = 0;

try {
  replayRecords = loadCsvDataset(config.datasetPath);
  console.log("Replay dataset loaded", { records: replayRecords.length });
} catch (error) {
  console.warn("Replay dataset unavailable; stream control disabled:", error.message);
}

function startStream() {
  if (state.streaming || !replayRecords || replayRecords.length === 0) return false;
  state.streaming = true;
  const intervalMs = Math.max(1, Math.round(1000 / config.streamRate));
  streamTimer = setInterval(() => {
    const reading = createSensorReading(replayRecords[streamIndex % replayRecords.length]);
    streamIndex += 1;
    client.publish(config.readingsTopic, JSON.stringify(reading), { qos: config.qos });
  }, intervalMs);
  console.log("Live stream started", { rate: config.streamRate });
  broadcast({ type: "stream", streaming: true, totals: totals() });
  return true;
}

function stopStream() {
  if (!state.streaming) return false;
  state.streaming = false;
  clearInterval(streamTimer);
  streamTimer = null;
  console.log("Live stream stopped");
  broadcast({ type: "stream", streaming: false, totals: totals() });
  return true;
}

// Raw sensor reading -> update window + ask MaaS for a prediction.
async function handleReading(reading) {
  const temperature = Number(reading.temperature);
  const humidity = Number(reading.humidity);
  if (!Number.isFinite(temperature) || !Number.isFinite(humidity)) return;

  state.totalReadings += 1;
  windowCount += 1;
  windowTempSum += temperature;
  windowHumSum += humidity;

  // Raw readings are high-volume: sample under load instead of queueing.
  const prediction = await predict(temperature, humidity, { skipIfBusy: true });

  const item = {
    deviceId: reading.deviceId,
    temperature,
    humidity,
    createdAt: reading.createdAt,
    receivedAt: new Date().toISOString(),
    prediction: prediction
      ? { class: prediction.predicted_class, confidence: prediction.confidence }
      : null
  };
  pushCapped(state.recentReadings, item);
  broadcast({ type: "reading", item, totals: totals() });
}

// eKuiper CEP detection -> record + enrich with an ML prediction.
async function handleCepEvent(event) {
  const eventType = event.eventType || "UNKNOWN";
  state.totalEvents += 1;
  state.eventCounts[eventType] = (state.eventCounts[eventType] || 0) + 1;

  // Windowed CEP rules emit avgTemperature/avgHumidity; per-event rules emit
  // temperature/humidity. Accept either so every event can be scored.
  const temperature = Number(event.temperature ?? event.avgTemperature);
  const humidity = Number(event.humidity ?? event.avgHumidity);
  let prediction = null;
  if (Number.isFinite(temperature) && Number.isFinite(humidity)) {
    // CEP events are lower-volume and meaningful: always score them.
    prediction = await predict(temperature, humidity);
  }

  const item = {
    eventType,
    description: event.description || null,
    deviceId: event.deviceId,
    temperature: Number.isFinite(temperature) ? temperature : null,
    humidity: Number.isFinite(humidity) ? humidity : null,
    samples: event.samples ?? null,
    detectedAt: new Date().toISOString(),
    prediction: prediction
      ? { class: prediction.predicted_class, confidence: prediction.confidence }
      : null
  };
  pushCapped(state.recentEvents, item);

  // Republish the enriched (CEP + ML) event to the analytics topic.
  client.publish(config.analyticsTopic, JSON.stringify(item), { qos: config.qos });
  broadcast({ type: "event", item, totals: totals() });
}

// ---------------------------------------------------------------------------
// Tumbling-window analytics (kept from Project 2, now also broadcast).
// ---------------------------------------------------------------------------
setInterval(() => {
  const windowEnd = Date.now();
  const avgTemperature = windowCount > 0 ? windowTempSum / windowCount : 0;
  const avgHumidity = windowCount > 0 ? windowHumSum / windowCount : 0;
  // Cold-average alert: the marine buoy stream is cold, so a meaningful warning
  // is a window whose average air temperature drops below the threshold.
  const alert = windowCount > 0 && avgTemperature < config.alertThreshold;

  const window = {
    windowStart: new Date(windowStart).toISOString(),
    windowEnd: new Date(windowEnd).toISOString(),
    messageCount: windowCount,
    avgTemperature: Number(avgTemperature.toFixed(2)),
    avgHumidity: Number(avgHumidity.toFixed(2)),
    alert
  };

  if (windowCount > 0) {
    state.lastWindow = window;
    pushCapped(state.windows, window);
    if (alert) console.log("ALERT window average temperature below threshold", window);
    broadcast({ type: "window", item: window, totals: totals() });
  }

  windowStart = windowEnd;
  windowCount = 0;
  windowTempSum = 0;
  windowHumSum = 0;
}, config.windowSeconds * 1000);

// ---------------------------------------------------------------------------
// HTTP + WebSocket API + static dashboard.
// ---------------------------------------------------------------------------
function totals() {
  return {
    streaming: state.streaming,
    totalReadings: state.totalReadings,
    totalEvents: state.totalEvents,
    totalPredictions: state.totalPredictions,
    predictionErrors: state.predictionErrors,
    predictionsSkipped: state.predictionsSkipped,
    maasAvailable: state.maasAvailable,
    eventCounts: state.eventCounts,
    mlClassCounts: state.mlClassCounts
  };
}

const app = express();
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok", startedAt: state.startedAt, maasAvailable: state.maasAvailable });
});

app.get("/api/summary", (_req, res) => {
  res.json({
    ...totals(),
    startedAt: state.startedAt,
    lastWindow: state.lastWindow,
    config: {
      readingsTopic: config.readingsTopic,
      eventsTopic: config.eventsTopic,
      analyticsTopic: config.analyticsTopic,
      windowSeconds: config.windowSeconds,
      alertThreshold: config.alertThreshold
    }
  });
});

app.get("/api/readings", (_req, res) => res.json(state.recentReadings));
app.get("/api/events", (_req, res) => res.json(state.recentEvents));
app.get("/api/windows", (_req, res) => res.json(state.windows));

// Dashboard controls for the live replay stream.
app.post("/api/stream/start", (_req, res) => {
  const started = startStream();
  res.json({ streaming: state.streaming, rate: config.streamRate, changed: started });
});
app.post("/api/stream/stop", (_req, res) => {
  const stopped = stopStream();
  res.json({ streaming: state.streaming, changed: stopped });
});

// Proxy the MaaS model metadata so the dashboard can show the model card.
app.get("/api/model", async (_req, res) => {
  try {
    const info = await callMaas("/model/info");
    res.json(info);
  } catch (error) {
    res.status(502).json({ error: error.message });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(payload);
  }
}

wss.on("connection", (socket) => {
  socket.send(JSON.stringify({ type: "snapshot", totals: totals(), lastWindow: state.lastWindow }));
});

server.listen(config.httpPort, () => {
  console.log(`Analytics v3 dashboard + API listening on :${config.httpPort}`);
});

// ---------------------------------------------------------------------------
// Graceful shutdown.
// ---------------------------------------------------------------------------
function shutdown(signal) {
  console.log("Analytics v3 shutting down", { signal });
  stopStream();
  server.close();
  client.end(false, {}, () => process.exit(0));
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
