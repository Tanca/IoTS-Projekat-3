const { randomUUID } = require("node:crypto");

function createSensorReading(record) {
  return {
    messageId: randomUUID(),
    deviceId: record.deviceId,
    temperature: record.temperature,
    humidity: record.humidity,
    createdAt: record.createdAt
  };
}

module.exports = {
  createSensorReading
};
