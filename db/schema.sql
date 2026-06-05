CREATE TABLE EVENTS (
  id UUID PRIMARY KEY,
  type TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_events_type ON EVENTS (type);
CREATE INDEX idx_events_timestamp ON EVENTS (timestamp);