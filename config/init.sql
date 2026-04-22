CREATE TABLE IF NOT EXISTS measurements (
    id INTEGER PRIMARY KEY,
    sensor_name TEXT,
    reading DOUBLE,
    recorded_at TIMESTAMP DEFAULT current_timestamp
);