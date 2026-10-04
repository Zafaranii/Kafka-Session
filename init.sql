CREATE TABLE sync_notifications (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE stream_notifications (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL UNIQUE,
    message TEXT NOT NULL,
    -- clock_timestamp(), not now(): now() is the start of the sink connector's
    -- batch transaction, which would make every row in a batch look saved
    -- before it was. The load test reads this to time the Postgres step.
    created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);
