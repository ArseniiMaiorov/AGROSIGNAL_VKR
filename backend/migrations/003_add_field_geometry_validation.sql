CREATE OR REPLACE FUNCTION validate_field_geometry()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
DECLARE
    validation_reason TEXT;
BEGIN
    IF NEW.geom IS NULL THEN
        RAISE EXCEPTION 'Геометрия поля обязательна';
    END IF;

    IF GeometryType(NEW.geom) <> 'POLYGON' THEN
        RAISE EXCEPTION 'Допустимы только полигоны';
    END IF;

    IF ST_IsEmpty(NEW.geom) THEN
        RAISE EXCEPTION 'Полигон пустой';
    END IF;

    IF ST_SRID(NEW.geom) <> 4326 THEN
        RAISE EXCEPTION 'Неверная система координат: ожидается EPSG:4326';
    END IF;

    IF NOT ST_IsValid(NEW.geom) THEN
        validation_reason := ST_IsValidReason(NEW.geom);

        IF POSITION('Self-intersection' IN validation_reason) > 0 THEN
            RAISE EXCEPTION 'Полигон самопересекается';
        END IF;

        RAISE EXCEPTION 'Невалидная геометрия поля: %', validation_reason;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_field_geometry ON fields;

CREATE TRIGGER trg_validate_field_geometry
    BEFORE INSERT OR UPDATE
    ON fields
    FOR EACH ROW
EXECUTE FUNCTION validate_field_geometry();
