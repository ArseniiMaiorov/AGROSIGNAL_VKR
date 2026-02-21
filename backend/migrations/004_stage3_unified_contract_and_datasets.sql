CREATE TABLE IF NOT EXISTS provider_sync_status (
    source TEXT PRIMARY KEY,
    last_sync_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'never',
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_provider_sync_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_provider_sync_status CHECK (status IN ('never', 'ok', 'error'))
);

CREATE TABLE IF NOT EXISTS provider_observations (
    id BIGSERIAL PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    metric_code TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_provider_observations_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_provider_observations_metric CHECK (
        metric_code IN ('precipitation', 'temperature', 'wind_speed', 'cloudiness', 'ndvi')
    ),
    CONSTRAINT uq_provider_observations UNIQUE (field_id, metric_code, observed_at, source)
);

CREATE INDEX IF NOT EXISTS idx_provider_observations_field_time
    ON provider_observations (field_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_provider_observations_source_time
    ON provider_observations (source, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_provider_observations_metric
    ON provider_observations (metric_code);

CREATE TABLE IF NOT EXISTS dataset_slices (
    dataset_id TEXT PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    granularity TEXT NOT NULL,
    export_format TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    request_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days'),
    export_status TEXT NOT NULL DEFAULT 'queued',
    export_error TEXT,
    export_file_path TEXT,
    warned_at TIMESTAMPTZ,
    last_accessed_at TIMESTAMPTZ,
    extended_count INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT chk_dataset_slices_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_dataset_slices_granularity CHECK (granularity IN ('month', 'day', 'hour', 'point')),
    CONSTRAINT chk_dataset_slices_format CHECK (export_format IN ('json', 'csv')),
    CONSTRAINT chk_dataset_slices_export_status CHECK (
        export_status IN ('queued', 'processing', 'ready', 'failed')
    ),
    CONSTRAINT chk_dataset_slices_range CHECK (range_end >= range_start)
);

CREATE INDEX IF NOT EXISTS idx_dataset_slices_expires_at
    ON dataset_slices (expires_at);

CREATE INDEX IF NOT EXISTS idx_dataset_slices_status
    ON dataset_slices (export_status);

CREATE TABLE IF NOT EXISTS dataset_notifications (
    id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES dataset_slices (dataset_id) ON DELETE CASCADE,
    channel TEXT NOT NULL DEFAULT 'ui',
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    read_at TIMESTAMPTZ,
    CONSTRAINT chk_dataset_notifications_channel CHECK (channel IN ('ui', 'email'))
);

CREATE INDEX IF NOT EXISTS idx_dataset_notifications_dataset_id
    ON dataset_notifications (dataset_id);
