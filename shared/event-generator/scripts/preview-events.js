#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { EventGenerator, loadCsvDataset } = require("../src");

function getArgumentValue(name, fallback) {
  const index = process.argv.indexOf(name);

  if (index === -1 || index === process.argv.length - 1) {
    return fallback;
  }

  return process.argv[index + 1];
}

function resolveDatasetPath() {
  const explicitPath = getArgumentValue("--dataset", null);

  if (explicitPath) {
    return path.resolve(process.cwd(), explicitPath);
  }

  const sampleDataDir = path.resolve(__dirname, "..", "..", "sample-data");
  const preferredPath = path.join(sampleDataDir, "sensor-data.csv");
  const existingDatasetPath = path.join(sampleDataDir, "marine_buoy_readings.csv");

  return fs.existsSync(preferredPath) ? preferredPath : existingDatasetPath;
}

function parsePositiveInteger(value, argumentName) {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${argumentName} must be a positive integer.`);
  }

  return parsed;
}

function main() {
  const count = parsePositiveInteger(getArgumentValue("--count", "5"), "--count");
  const deviceCountValue = getArgumentValue("--device-count", null);
  const deviceCount = deviceCountValue
    ? parsePositiveInteger(deviceCountValue, "--device-count")
    : undefined;
  const datasetPath = resolveDatasetPath();
  const records = loadCsvDataset(datasetPath);
  const generator = new EventGenerator(records);
  const events = generator.generateEvents({
    totalMessages: count,
    deviceCount
  });

  console.log(JSON.stringify(events, null, 2));
}

try {
  main();
} catch (error) {
  console.error(`Preview failed: ${error.message}`);
  process.exitCode = 1;
}
