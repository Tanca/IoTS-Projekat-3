const fs = require("node:fs");
const path = require("node:path");

const REQUIRED_FIELDS = ["sensor_id", "timestamp", "temperature_C", "humidity_%"];

function parseCsv(content) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const nextChar = content[index + 1];

    if (char === "\"") {
      if (inQuotes && nextChar === "\"") {
        field += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && nextChar === "\n") {
        index += 1;
      }
      row.push(field);
      if (row.some((value) => value.trim() !== "")) {
        rows.push(row);
      }
      row = [];
      field = "";
      continue;
    }

    field += char;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    if (row.some((value) => value.trim() !== "")) {
      rows.push(row);
    }
  }

  return rows;
}

function normalizeTimestamp(value) {
  const trimmed = value.trim();
  const parsed = new Date(trimmed);

  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString();
}

function toNumber(value) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function validateHeaders(headers) {
  const missingFields = REQUIRED_FIELDS.filter((field) => !headers.includes(field));

  if (missingFields.length > 0) {
    throw new Error(`CSV is missing required fields: ${missingFields.join(", ")}`);
  }
}

function rowToObject(headers, row) {
  return headers.reduce((record, header, index) => {
    record[header] = row[index] === undefined ? "" : row[index].trim();
    return record;
  }, {});
}

function normalizeRow(rawRow) {
  const deviceId = rawRow.sensor_id;
  const temperature = toNumber(rawRow.temperature_C);
  const humidity = toNumber(rawRow["humidity_%"]);
  const createdAt = normalizeTimestamp(rawRow.timestamp);

  if (!deviceId || temperature === null || humidity === null || !createdAt) {
    return null;
  }

  return {
    deviceId,
    temperature,
    humidity,
    createdAt
  };
}

function loadCsvDataset(filePath) {
  const resolvedPath = path.resolve(filePath);
  const content = fs.readFileSync(resolvedPath, "utf8");
  const rows = parseCsv(content);

  if (rows.length === 0) {
    return [];
  }

  const headers = rows[0].map((header) => header.trim());
  validateHeaders(headers);

  return rows
    .slice(1)
    .map((row) => rowToObject(headers, row))
    .map(normalizeRow)
    .filter(Boolean);
}

module.exports = {
  REQUIRED_FIELDS,
  loadCsvDataset,
  parseCsv
};
