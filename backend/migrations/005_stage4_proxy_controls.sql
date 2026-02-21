CREATE TABLE IF NOT EXISTS proxy_settings (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    proxy_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    proxy_mode TEXT NOT NULL DEFAULT 'global',
    copernicus_via_proxy BOOLEAN NOT NULL DEFAULT TRUE,
    nasa_via_proxy BOOLEAN NOT NULL DEFAULT TRUE,
    bypass_hosts JSONB NOT NULL DEFAULT '[]'::jsonb,
    bypass_policy TEXT NOT NULL DEFAULT 'direct',
    proxy_endpoint TEXT,
    timeout_seconds INTEGER NOT NULL DEFAULT 10,
    max_retries INTEGER NOT NULL DEFAULT 3,
    backoff_schedule JSONB NOT NULL DEFAULT '[1, 5, 15]'::jsonb,
    last_check_at TIMESTAMPTZ,
    last_check_result TEXT NOT NULL DEFAULT 'NEVER',
    last_check_reason TEXT,
    last_proxy_latency_ms INTEGER,
    last_source_latency_ms INTEGER,
    last_source_status INTEGER,
    source_reachability TEXT NOT NULL DEFAULT 'UNKNOWN',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    CONSTRAINT chk_proxy_settings_singleton CHECK (id = 1),
    CONSTRAINT chk_proxy_mode CHECK (proxy_mode IN ('global', 'per_provider')),
    CONSTRAINT chk_bypass_policy CHECK (bypass_policy IN ('direct', 'force_proxy')),
    CONSTRAINT chk_proxy_check_result CHECK (last_check_result IN ('NEVER', 'OK', 'FAIL')),
    CONSTRAINT chk_source_reachability CHECK (source_reachability IN ('UNKNOWN', 'OK', 'FAIL')),
    CONSTRAINT chk_proxy_timeout CHECK (timeout_seconds BETWEEN 1 AND 120),
    CONSTRAINT chk_proxy_retries CHECK (max_retries BETWEEN 0 AND 10)
);

INSERT INTO proxy_settings (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS provider_sync_journal (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    proxy_used BOOLEAN NOT NULL DEFAULT FALSE,
    request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_provider_sync_journal_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_provider_sync_journal_status CHECK (status IN ('ok', 'fail'))
);

CREATE INDEX IF NOT EXISTS idx_provider_sync_journal_source_time
    ON provider_sync_journal (source, created_at DESC);

CREATE TABLE IF NOT EXISTS proxy_request_logs (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    module_name TEXT NOT NULL,
    proxy_used BOOLEAN NOT NULL,
    target_host TEXT NOT NULL,
    http_status INTEGER,
    bytes_downloaded BIGINT NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL,
    error_class TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL,
    CONSTRAINT chk_proxy_request_logs_provider CHECK (provider IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_proxy_request_logs_duration CHECK (duration_ms >= 0),
    CONSTRAINT chk_proxy_request_logs_retry CHECK (retry_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_proxy_request_logs_reqid
    ON proxy_request_logs (request_id);

CREATE INDEX IF NOT EXISTS idx_proxy_request_logs_provider_time
    ON proxy_request_logs (provider, created_at DESC);
