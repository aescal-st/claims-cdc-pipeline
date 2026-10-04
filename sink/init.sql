CREATE TABLE IF NOT EXISTS claims (
  claim_id SERIAL PRIMARY KEY,
  member_id VARCHAR(20) NOT NULL,
  provider_id VARCHAR(20) NOT NULL,
  diagnosis_code VARCHAR(10),
  amount NUMERIC(10,2),
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE claims REPLICA IDENTITY FULL;
