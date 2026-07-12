const { createSensorReading } = require("./sensorReading");

function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function uniqueDeviceIds(records) {
  return Array.from(new Set(records.map((record) => record.deviceId)));
}

function filterByDeviceCount(records, deviceCount) {
  if (!deviceCount) {
    return records;
  }

  if (!Number.isInteger(deviceCount) || deviceCount < 1) {
    throw new Error("deviceCount must be a positive integer when provided.");
  }

  const selectedDevices = new Set(uniqueDeviceIds(records).slice(0, deviceCount));
  return records.filter((record) => selectedDevices.has(record.deviceId));
}

class EventGenerator {
  constructor(records) {
    if (!Array.isArray(records) || records.length === 0) {
      throw new Error("EventGenerator requires at least one normalized dataset record.");
    }

    this.records = records;
  }

  getRecordsForSimulation(options = {}) {
    const records = filterByDeviceCount(this.records, options.deviceCount);

    if (records.length === 0) {
      throw new Error("No dataset records are available for the requested simulation controls.");
    }

    return records;
  }

  generateEvents(options = {}) {
    const totalMessages = options.totalMessages;

    if (!Number.isInteger(totalMessages) || totalMessages < 0) {
      throw new Error("totalMessages must be a non-negative integer.");
    }

    const records = this.getRecordsForSimulation(options);
    const events = [];

    for (let index = 0; index < totalMessages; index += 1) {
      events.push(createSensorReading(records[index % records.length]));
    }

    return events;
  }

  async *streamEvents(options = {}) {
    const messagesPerSecond = options.messagesPerSecond;

    if (!Number.isFinite(messagesPerSecond) || messagesPerSecond <= 0) {
      throw new Error("messagesPerSecond must be a positive number.");
    }

    const records = this.getRecordsForSimulation(options);
    const intervalMs = 1000 / messagesPerSecond;
    let index = 0;

    while (true) {
      yield createSensorReading(records[index % records.length]);
      index += 1;
      await delay(intervalMs);
    }
  }
}

module.exports = {
  EventGenerator,
  filterByDeviceCount
};
