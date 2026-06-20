CREATE TABLE IF NOT EXISTS EVENTS (
  id UUID PRIMARY KEY,
  type TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type ON EVENTS (type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON EVENTS (timestamp);

CREATE TABLE IF NOT EXISTS FRAUD_ALERTS (
  alert_id UUID PRIMARY KEY,
  account_id TEXT NOT NULL,
  event_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  transaction_count INT NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_account_id ON FRAUD_ALERTS (alert_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_detected_at ON FRAUD_ALERTS (detected_at);
