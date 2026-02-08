-- SQLite schema for Mazonda Gas Industrial Ingestion Pipeline

-- Table to track ingestion files with deduplication
CREATE TABLE IF NOT EXISTS ingestion_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT DEFAULT 'imap' NOT NULL,
    message_id TEXT,
    from_email TEXT,
    subject TEXT,
    received_at DATETIME,
    filename TEXT,
    file_sha256 TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'NEW' NOT NULL CHECK (status IN ('NEW', 'PROCESSING', 'COMPLETED', 'FAILED', 'DUPLICATE')),
    error TEXT,
    processing_started_at DATETIME,
    processing_completed_at DATETIME,
    correlation_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))) || '-' || lower(hex(randomblob(8))) || '-4' || substr(lower(hex(randomblob(8))), 2) || '-' || substr('89ab', abs(random() % 4) + 1, 1) || substr(lower(hex(randomblob(8))), 2) || '-' || lower(hex(randomblob(12)))),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Table to store normalized scale transaction data
CREATE TABLE IF NOT EXISTS scale_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scale_name TEXT NOT NULL,
    transact_no INTEGER NOT NULL,
    cyl_size_kg REAL,
    tare_weight_kg REAL,
    fill_kg REAL,
    residual_kg REAL,
    success BOOLEAN,
    started_at DATETIME,
    fill_time_seconds INTEGER,
    ingestion_file_id INTEGER NOT NULL,
    correlation_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(16))) || '-' || lower(hex(randomblob(8))) || '-4' || substr(lower(hex(randomblob(8))), 2) || '-' || substr('89ab', abs(random() % 4) + 1, 1) || substr(lower(hex(randomblob(8))), 2) || '-' || lower(hex(randomblob(12))),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE(scale_name, transact_no),
    FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ingestion_files_file_sha256 ON ingestion_files(file_sha256);
CREATE INDEX IF NOT EXISTS idx_ingestion_files_status ON ingestion_files(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_files_received_at ON ingestion_files(received_at);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_scale_name ON scale_transactions(scale_name);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_transact_no ON scale_transactions(transact_no);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_ingestion_file_id ON scale_transactions(ingestion_file_id);
CREATE INDEX IF NOT EXISTS idx_scale_transactions_started_at ON scale_transactions(started_at);

-- Triggers to update updated_at timestamp
CREATE TRIGGER IF NOT EXISTS update_ingestion_files_updated_at 
    AFTER UPDATE ON ingestion_files
    FOR EACH ROW
    BEGIN
        UPDATE ingestion_files SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
    
CREATE TRIGGER IF NOT EXISTS update_scale_transactions_updated_at 
    AFTER UPDATE ON scale_transactions
    FOR EACH ROW
    BEGIN
        UPDATE scale_transactions SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
