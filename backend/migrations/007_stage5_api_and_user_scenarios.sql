ALTER TABLE enterprises
    ADD COLUMN IF NOT EXISTS owner_user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL;

ALTER TABLE enterprises
    DROP CONSTRAINT IF EXISTS enterprises_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_enterprises_owner_name
    ON enterprises (COALESCE(owner_user_id, 0), lower(name));

INSERT INTO roles (code, name)
VALUES
    ('admin', 'Администратор'),
    ('manager', 'Менеджер'),
    ('agronomist', 'Агроном'),
    ('viewer', 'Наблюдатель')
ON CONFLICT (code) DO NOTHING;

ALTER TABLE fields
    ADD COLUMN IF NOT EXISTS bbox GEOMETRY(POLYGON, 4326),
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE fields
SET bbox = ST_Envelope(ST_Force2D(geom))::geometry(POLYGON, 4326),
    updated_at = NOW()
WHERE bbox IS NULL;

CREATE INDEX IF NOT EXISTS idx_fields_deleted_at ON fields (deleted_at);
CREATE INDEX IF NOT EXISTS idx_fields_bbox_gist ON fields USING GIST (bbox);

CREATE OR REPLACE FUNCTION set_field_stage5_derived()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN
    IF NEW.geom IS NULL THEN
        RAISE EXCEPTION 'Геометрия поля обязательна';
    END IF;

    IF ST_Area(NEW.geom::geography) <= 0 THEN
        RAISE EXCEPTION 'Площадь поля должна быть больше 0';
    END IF;

    NEW.bbox := ST_Envelope(ST_Force2D(NEW.geom))::geometry(POLYGON, 4326);
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_field_stage5_derived ON fields;

CREATE TRIGGER trg_set_field_stage5_derived
    BEFORE INSERT OR UPDATE OF geom, name, deleted_at
    ON fields
    FOR EACH ROW
EXECUTE FUNCTION set_field_stage5_derived();

CREATE TABLE IF NOT EXISTS field_geometry_history (
    id BIGSERIAL PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    changed_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    request_id TEXT,
    old_geom GEOMETRY(POLYGON, 4326),
    new_geom GEOMETRY(POLYGON, 4326) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_field_geometry_history_field_id
    ON field_geometry_history (field_id, changed_at DESC);

ALTER TABLE seasons
    ADD COLUMN IF NOT EXISTS field_id BIGINT REFERENCES fields (id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN IF NOT EXISTS close_reason TEXT;

ALTER TABLE seasons
    DROP CONSTRAINT IF EXISTS chk_season_status;

ALTER TABLE seasons
    ADD CONSTRAINT chk_season_status CHECK (status IN ('active', 'archived'));

CREATE INDEX IF NOT EXISTS idx_seasons_field_id ON seasons (field_id);
CREATE INDEX IF NOT EXISTS idx_seasons_status ON seasons (status);

CREATE TABLE IF NOT EXISTS field_operations (
    id BIGSERIAL PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    operation_type TEXT NOT NULL,
    operation_at TIMESTAMPTZ NOT NULL,
    comment TEXT,
    point_geom GEOMETRY(POINT, 4326),
    zone_geom GEOMETRY(POLYGON, 4326),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_field_operations_type CHECK (
        operation_type IN ('irrigation', 'treatment', 'sowing', 'harvest', 'inspection', 'other')
    )
);

CREATE INDEX IF NOT EXISTS idx_field_operations_field_time
    ON field_operations (field_id, operation_at DESC);

CREATE TABLE IF NOT EXISTS assistant_rules (
    id BIGSERIAL PRIMARY KEY,
    enterprise_id BIGINT NOT NULL REFERENCES enterprises (id) ON DELETE CASCADE,
    field_id BIGINT REFERENCES fields (id) ON DELETE CASCADE,
    parameter TEXT NOT NULL,
    condition_code TEXT NOT NULL,
    threshold_value DOUBLE PRECISION,
    threshold_min DOUBLE PRECISION,
    threshold_max DOUBLE PRECISION,
    period_hours INTEGER NOT NULL DEFAULT 24,
    recommendation_text TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warn',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    updated_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_assistant_rules_parameter CHECK (
        parameter IN ('wind', 'precipitation', 'temperature', 'frost')
    ),
    CONSTRAINT chk_assistant_rules_condition CHECK (
        condition_code IN ('gt', 'lt', 'between')
    ),
    CONSTRAINT chk_assistant_rules_period CHECK (period_hours BETWEEN 1 AND 720),
    CONSTRAINT chk_assistant_rules_severity CHECK (severity IN ('info', 'warn', 'critical')),
    CONSTRAINT chk_assistant_rules_between CHECK (
        (condition_code <> 'between')
        OR (threshold_min IS NOT NULL AND threshold_max IS NOT NULL AND threshold_max >= threshold_min)
    )
);

CREATE INDEX IF NOT EXISTS idx_assistant_rules_scope
    ON assistant_rules (enterprise_id, field_id, is_active);

CREATE TABLE IF NOT EXISTS assistant_decision_journal (
    id BIGSERIAL PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    rule_id BIGINT REFERENCES assistant_rules (id) ON DELETE SET NULL,
    user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    decision TEXT NOT NULL,
    recommendation_text TEXT,
    reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_id TEXT,
    shown_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_assistant_decision CHECK (decision IN ('shown', 'confirmed', 'rejected'))
);

CREATE INDEX IF NOT EXISTS idx_assistant_decision_field_time
    ON assistant_decision_journal (field_id, shown_at DESC);

CREATE TABLE IF NOT EXISTS api_audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT,
    before_state JSONB,
    after_state JSONB,
    request_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_audit_log_created_at ON api_audit_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_audit_log_user_id ON api_audit_log (user_id);

CREATE TABLE IF NOT EXISTS api_request_log (
    id BIGSERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_api_request_status CHECK (status_code BETWEEN 100 AND 599),
    CONSTRAINT chk_api_request_duration CHECK (duration_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_api_request_log_created_at ON api_request_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_request_log_error_code ON api_request_log (error_code);

CREATE TABLE IF NOT EXISTS api_idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    endpoint TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_payload JSONB NOT NULL,
    status_code INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_idempotency_endpoint ON api_idempotency_keys (endpoint, created_at DESC);

CREATE TABLE IF NOT EXISTS api_export_jobs (
    export_id TEXT PRIMARY KEY,
    entity TEXT NOT NULL,
    source TEXT NOT NULL,
    field_ids JSONB NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    granularity TEXT NOT NULL,
    export_format TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    file_path TEXT,
    error_text TEXT,
    request_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    created_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days'),
    warned_at TIMESTAMPTZ,
    extended_count INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT chk_api_export_entity CHECK (entity IN ('weather', 'satellite', 'assistant')),
    CONSTRAINT chk_api_export_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_api_export_granularity CHECK (granularity IN ('month', 'day', 'hour', 'point')),
    CONSTRAINT chk_api_export_format CHECK (export_format IN ('json', 'csv')),
    CONSTRAINT chk_api_export_status CHECK (status IN ('pending', 'running', 'done', 'failed')),
    CONSTRAINT chk_api_export_range CHECK (range_end >= range_start)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_api_export_jobs_idempotency_key
    ON api_export_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_api_export_jobs_status
    ON api_export_jobs (status, created_at);

CREATE INDEX IF NOT EXISTS idx_api_export_jobs_expires_at
    ON api_export_jobs (expires_at);
