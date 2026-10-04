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
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
