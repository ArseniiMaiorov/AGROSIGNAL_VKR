CREATE TABLE IF NOT EXISTS api_layer_registry (
    layer_id TEXT NOT NULL,
    source TEXT NOT NULL,
    title_ru TEXT NOT NULL,
    category TEXT NOT NULL,
    value_type TEXT NOT NULL,
    units TEXT NOT NULL,
    time_available TEXT[] NOT NULL DEFAULT ARRAY['hour'],
    default_granularity TEXT NOT NULL DEFAULT 'hour',
    max_lookback_days INTEGER NOT NULL DEFAULT 30,
    spatial_modes TEXT[] NOT NULL DEFAULT ARRAY['grid'],
    zoom_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
    grid_sizes_m INTEGER[] NOT NULL DEFAULT ARRAY[1000],
    legend JSONB NOT NULL DEFAULT '{}'::jsonb,
    has_quality_flags BOOLEAN NOT NULL DEFAULT TRUE,
    quality_rules TEXT,
    status TEXT NOT NULL DEFAULT 'OK',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_api_layer_registry PRIMARY KEY (layer_id, source),
    CONSTRAINT chk_api_layer_registry_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_api_layer_registry_category CHECK (
        category IN ('weather', 'atmo_dynamics', 'precip', 'soil', 'satellite', 'risk', 'scenario')
    ),
    CONSTRAINT chk_api_layer_registry_value_type CHECK (value_type IN ('scalar', 'vector', 'raster', 'contour')),
    CONSTRAINT chk_api_layer_registry_default_granularity CHECK (
        default_granularity IN ('month', 'day', 'hour')
    ),
    CONSTRAINT chk_api_layer_registry_max_lookback_days CHECK (max_lookback_days BETWEEN 1 AND 3650),
    CONSTRAINT chk_api_layer_registry_status CHECK (status IN ('OK', 'DEGRADED', 'DOWN'))
);

CREATE TABLE IF NOT EXISTS api_field_zones (
    zone_id TEXT PRIMARY KEY,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    method TEXT NOT NULL,
    zoom INTEGER NOT NULL,
    zone_rank INTEGER NOT NULL,
    zone_geom GEOMETRY(POLYGON, 4326) NOT NULL,
    heterogeneity JSONB NOT NULL DEFAULT '{}'::jsonb,
    stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_for TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_api_field_zones_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_api_field_zones_method CHECK (method IN ('grid', 'quantiles', 'kmeans')),
    CONSTRAINT chk_api_field_zones_zoom CHECK (zoom BETWEEN 0 AND 22)
);

CREATE INDEX IF NOT EXISTS idx_api_field_zones_field_generated
    ON api_field_zones (field_id, source, method, zoom, generated_for DESC);
CREATE INDEX IF NOT EXISTS idx_api_field_zones_geom
    ON api_field_zones USING GIST (zone_geom);

CREATE TABLE IF NOT EXISTS api_scenarios (
    scenario_id TEXT PRIMARY KEY,
    baseline_id TEXT NOT NULL,
    field_id BIGINT NOT NULL REFERENCES fields (id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'draft',
    result_payload JSONB,
    diff_payload JSONB,
    assumptions JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_text TEXT,
    created_by BIGINT REFERENCES app_users (id) ON DELETE SET NULL,
    request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_api_scenarios_source CHECK (source IN ('Copernicus', 'NASA', 'Mock')),
    CONSTRAINT chk_api_scenarios_status CHECK (status IN ('draft', 'running', 'done', 'failed')),
    CONSTRAINT chk_api_scenarios_range CHECK (range_end >= range_start)
);

CREATE INDEX IF NOT EXISTS idx_api_scenarios_field_created
    ON api_scenarios (field_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_scenarios_status
    ON api_scenarios (status, updated_at DESC);

WITH sources(source) AS (
    VALUES ('Copernicus'), ('NASA'), ('Mock')
),
layers(layer_id, title_ru, category, value_type, units, time_available, default_granularity, spatial_modes, grid_sizes_m, zoom_rules, legend, quality_rules) AS (
    VALUES
        (
            'weather.wind_vector_10m',
            'Ветер (направление и скорость, 10 м)',
            'atmo_dynamics',
            'vector',
            'м/с',
            ARRAY['hour','day']::text[],
            'hour',
            ARRAY['grid','tiles']::text[],
            ARRAY[1000,500,250,100]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"speed_scale","min":0,"max":25}'::jsonb,
            'Скорость ветра учитывает качество и полноту ряда'
        ),
        (
            'weather.temperature_2m',
            'Температура воздуха (2 м)',
            'weather',
            'scalar',
            'C',
            ARRAY['hour','day','month']::text[],
            'day',
            ARRAY['field','grid','tiles']::text[],
            ARRAY[1000,500,250]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"gradient","min":-30,"max":45}'::jsonb,
            'Температура агрегируется по выбранной гранулярности'
        ),
        (
            'weather.precipitation',
            'Осадки',
            'precip',
            'scalar',
            'mm',
            ARRAY['hour','day','month']::text[],
            'day',
            ARRAY['field','grid','tiles']::text[],
            ARRAY[1000,500,250]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"bins","thresholds":[0,1,5,10,20,40]}'::jsonb,
            'Осадки учитывают пропуски и облачность источника'
        ),
        (
            'satellite.ndvi',
            'NDVI',
            'satellite',
            'raster',
            'index',
            ARRAY['day','month']::text[],
            'day',
            ARRAY['grid','tiles','zones']::text[],
            ARRAY[1000,500,250,100]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"ndvi","min":0,"max":1}'::jsonb,
            'NDVI помечается как недостоверный при высокой облачности'
        ),
        (
            'satellite.ndre',
            'NDRE',
            'satellite',
            'raster',
            'index',
            ARRAY['day','month']::text[],
            'day',
            ARRAY['grid','tiles','zones']::text[],
            ARRAY[1000,500,250,100]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"ndre","min":0,"max":1}'::jsonb,
            'NDRE помечается как недостоверный при высокой облачности'
        ),
        (
            'satellite.ndmi',
            'NDMI',
            'satellite',
            'raster',
            'index',
            ARRAY['day','month']::text[],
            'day',
            ARRAY['grid','tiles','zones']::text[],
            ARRAY[1000,500,250,100]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"ndmi","min":0,"max":1}'::jsonb,
            'NDMI помечается как недостоверный при высокой облачности'
        ),
        (
            'satellite.cloud_mask',
            'Облачность и маска качества',
            'risk',
            'contour',
            '%',
            ARRAY['hour','day']::text[],
            'hour',
            ARRAY['grid','tiles']::text[],
            ARRAY[1000,500,250]::integer[],
            '{"z<=9":"grid:1000","10-12":"grid:500",">=13":"tiles"}'::jsonb,
            '{"type":"cloudiness","min":0,"max":100}'::jsonb,
            'Качество сцены снижается при облачности выше 70%'
        )
)
INSERT INTO api_layer_registry (
    layer_id,
    source,
    title_ru,
    category,
    value_type,
    units,
    time_available,
    default_granularity,
    max_lookback_days,
    spatial_modes,
    grid_sizes_m,
    zoom_rules,
    legend,
    has_quality_flags,
    quality_rules,
    status
)
SELECT
    l.layer_id,
    s.source,
    l.title_ru,
    l.category,
    l.value_type,
    l.units,
    l.time_available,
    l.default_granularity,
    30,
    l.spatial_modes,
    l.grid_sizes_m,
    l.zoom_rules,
    l.legend,
    TRUE,
    l.quality_rules,
    'OK'
FROM layers l
CROSS JOIN sources s
ON CONFLICT (layer_id, source) DO UPDATE
SET
    title_ru = EXCLUDED.title_ru,
    category = EXCLUDED.category,
    value_type = EXCLUDED.value_type,
    units = EXCLUDED.units,
    time_available = EXCLUDED.time_available,
    default_granularity = EXCLUDED.default_granularity,
    max_lookback_days = EXCLUDED.max_lookback_days,
    spatial_modes = EXCLUDED.spatial_modes,
    grid_sizes_m = EXCLUDED.grid_sizes_m,
    zoom_rules = EXCLUDED.zoom_rules,
    legend = EXCLUDED.legend,
    has_quality_flags = EXCLUDED.has_quality_flags,
    quality_rules = EXCLUDED.quality_rules,
    status = EXCLUDED.status,
    updated_at = NOW();
