-- Initial schema for Mazonda Gas Industrial Ingestion Pipeline
-- This migration creates the core tables for file tracking and scale transactions

-- Enable UUID generation for correlation IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table to track ingestion files with deduplication
CREATE TABLE IF NOT EXISTS ingestion_files (
  id BIGSERIAL PRIMARY KEY,
  source TEXT DEFAULT 'imap' NOT NULL,
  message_id TEXT,
  from_email TEXT,
  subject TEXT,
  received_at TIMESTAMPTZ,
  filename TEXT,
  file_sha256 TEXT UNIQUE NOT NULL,
  status TEXT DEFAULT 'NEW' NOT NULL CHECK (status IN ('NEW', 'PROCESSING', 'COMPLETED', 'FAILED', 'DUPLICATE')),
  error TEXT,
  processing_started_at TIMESTAMPTZ,
  processing_completed_at TIMESTAMPTZ,
  correlation_id UUID DEFAULT uuid_generate_v4() NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Table to store normalized scale transaction data
CREATE TABLE IF NOT EXISTS scale_transactions (
  id BIGSERIAL PRIMARY KEY,
  scale_name TEXT NOT NULL,
  transact_no BIGINT NOT NULL,
  cyl_size_kg NUMERIC(10,3),
  tare_weight_kg NUMERIC(10,3),
  fill_kg NUMERIC(10,3),
  residual_kg NUMERIC(10,3),
  success BOOLEAN,
  started_at TIMESTAMPTZ,
  fill_time_seconds INTEGER,
  ingestion_file_id BIGINT NOT NULL REFERENCES ingestion_files(id) ON DELETE CASCADE,
  correlation_id UUID DEFAULT uuid_generate_v4() NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  UNIQUE(scale_name, transact_no)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ingestion_files_file_sha256 ON ingestion_files(file_sha256);
CREATE INDEX IF NOT EXISTS idx_ingestion_files_status ON ingestion_files(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_files_received_at ON ingestion_files(received_at);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_scale_name ON scale_transactions(scale_name);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_transact_no ON scale_transactions(transact_no);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_ingestion_file_id ON scale_transactions(ingestion_file_id);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_started_at ON scale_transactions(started_at);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_ingestion_files_updated_at BEFORE UPDATE
    ON ingestion_files FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_scale_transactions_updated_at BEFORE UPDATE
    ON scale_transactions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE ingestion_files IS 'Tracks CSV files processed through the ingestion pipeline with SHA256 deduplication';
COMMENT ON TABLE scale_transactions IS 'Stores normalized scale transaction data with row-level deduplication';
COMMENT ON COLUMN ingestion_files.file_sha256 IS 'SHA256 hash of the file content for deduplication';
COMMENT ON COLUMN ingestion_files.correlation_id IS 'UUID for tracing operations through the pipeline';
COMMENT ON COLUMN scale_transactions.correlation_id IS 'UUID for tracing individual transaction processing';
COMMENT ON COLUMN scale_transactions.started_at IS 'Transaction start time stored in UTC (converted from Africa/Johannesburg)';
