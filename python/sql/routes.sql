-- SQL to create the routes table
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    xpath TEXT NOT NULL,
    namespace JSONB NOT NULL,
    value TEXT NOT NULL,
    dest_topic TEXT NOT NULL
);
