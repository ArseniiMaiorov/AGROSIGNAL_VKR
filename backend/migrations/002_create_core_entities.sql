CREATE TABLE IF NOT EXISTS enterprises (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS roles (
    id SMALLSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_users (
    id BIGSERIAL PRIMARY KEY,
    enterprise_id BIGINT REFERENCES enterprises (id) ON DELETE SET NULL,
    role_id SMALLINT NOT NULL REFERENCES roles (id),
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crops (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seasons (
    id BIGSERIAL PRIMARY KEY,
    enterprise_id BIGINT NOT NULL REFERENCES enterprises (id) ON DELETE CASCADE,
    crop_id BIGINT NOT NULL REFERENCES crops (id),
    year INTEGER NOT NULL CHECK (year BETWEEN 2000 AND 2100),
    name TEXT NOT NULL,
    started_at DATE,
    ended_at DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_season_dates CHECK (ended_at IS NULL OR started_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE IF NOT EXISTS fields (
    id BIGSERIAL PRIMARY KEY,
    enterprise_id BIGINT NOT NULL REFERENCES enterprises (id) ON DELETE CASCADE,
    season_id BIGINT REFERENCES seasons (id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    geom GEOMETRY(POLYGON) NOT NULL,
    area_ha DOUBLE PRECISION GENERATED ALWAYS AS (
        ROUND((ST_Area(geom::geography) / 10000.0)::NUMERIC, 4)::DOUBLE PRECISION
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (enterprise_id, name)
);

CREATE INDEX IF NOT EXISTS idx_fields_geom_gist ON fields USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_fields_enterprise_id ON fields (enterprise_id);
CREATE INDEX IF NOT EXISTS idx_seasons_enterprise_id ON seasons (enterprise_id);

CREATE TABLE IF NOT EXISTS work_journal (
    id BIGSERIAL PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    user_id BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    work_type TEXT NOT NULL,
    description TEXT,
    planned_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_work_journal_field_id ON work_journal (field_id);
