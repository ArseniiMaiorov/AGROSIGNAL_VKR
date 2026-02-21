ALTER TABLE provider_observations
    DROP CONSTRAINT IF EXISTS chk_provider_observations_metric;

ALTER TABLE provider_observations
    ADD CONSTRAINT chk_provider_observations_metric CHECK (
        metric_code IN (
            'precipitation',
            'temperature',
            'wind_speed',
            'cloudiness',
            'ndvi',
            'ndre',
            'ndmi',
            'cloud_mask'
        )
    );
