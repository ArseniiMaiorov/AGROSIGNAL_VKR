CREATE OR REPLACE FUNCTION set_field_stage5_derived()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN
    IF NEW.geom IS NULL THEN
        RAISE EXCEPTION 'Геометрия поля обязательна';
    END IF;

    IF ST_SRID(NEW.geom) = 4326 THEN
        NEW.bbox := ST_Envelope(ST_Force2D(NEW.geom))::geometry(POLYGON, 4326);
    ELSE
        NEW.bbox := NULL;
    END IF;

    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;
