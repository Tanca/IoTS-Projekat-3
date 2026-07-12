CREATE TABLE IF NOT EXISTS sensor_readings (
    id UUID PRIMARY KEY,
    message_id VARCHAR(100) NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    created_at TIMESTAMP,
    broker_type VARCHAR(20),
    delivery_mode VARCHAR(20),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
