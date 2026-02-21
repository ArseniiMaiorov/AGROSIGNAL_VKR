from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXPORT_DIR = ROOT / "exports"
CONTRACT_VERSION = "v1.0-stage3"
VALID_SOURCES = {"Copernicus", "NASA", "Mock"}
VALID_GRANULARITIES = {"month", "day", "hour", "point"}
VALID_EXPORT_FORMATS = {"json", "csv"}
METRICS: dict[str, tuple[str, str]] = {
    "precipitation": ("Осадки", "mm"),
    "temperature": ("Температура", "C"),
    "wind_speed": ("Скорость ветра", "m/s"),
    "cloudiness": ("Облачность", "%"),
    "ndvi": ("NDVI", "index"),
}


class Stage3Error(RuntimeError):
    pass


@dataclass
class DbResult:
    stdout: str
    stderr: str
    returncode: int


class DbClient:
    def __init__(self) -> None:
        self.database_url = os.environ.get("DATABASE_URL")
        self.compose_cmd = ["docker", "compose", "-f", "docker-compose.yml"]

    def ensure_ready(self) -> None:
        if self.database_url:
            return
        result = subprocess.run(
            self.compose_cmd + ["up", "-d", "--wait", "db"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise Stage3Error(result.stderr.strip() or result.stdout.strip())

    def _psql_base(self) -> list[str]:
        if self.database_url:
            return ["psql", self.database_url, "-X", "-v", "ON_ERROR_STOP=1"]
        return self.compose_cmd + [
            "exec",
            "-T",
            "db",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "zemledar",
            "-d",
            "zemledar",
        ]

    def exec(self, sql: str, *, tuples_only: bool = False) -> DbResult:
        cmd = self._psql_base()
        if tuples_only:
            cmd.extend(["-At", "-F", "|"])
        cmd.extend(["-f", "-"])

        result = subprocess.run(
            cmd,
            cwd=ROOT,
            text=True,
            input=sql,
            capture_output=True,
            check=False,
        )
        return DbResult(
            stdout=result.stdout.strip(),
            stderr=result.stderr.strip(),
            returncode=result.returncode,
        )

    def exec_checked(self, sql: str, *, tuples_only: bool = False) -> str:
        result = self.exec(sql, tuples_only=tuples_only)
        if result.returncode != 0:
            raise Stage3Error(result.stderr or result.stdout or "Ошибка выполнения SQL")
        return result.stdout

    def query_json(self, sql: str) -> Any:
        out = self.exec_checked(sql, tuples_only=True)
        if not out:
            return None
        return json.loads(out)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return _to_utc(parsed)


def _validate_source(source: str) -> None:
    if source not in VALID_SOURCES:
        raise Stage3Error(f"Неизвестный источник: {source}")


def _validate_granularity(granularity: str) -> None:
    if granularity not in VALID_GRANULARITIES:
        raise Stage3Error(f"Неподдерживаемая гранулярность: {granularity}")


def _validate_export_format(export_format: str) -> None:
    if export_format not in VALID_EXPORT_FORMATS:
        raise Stage3Error(f"Неподдерживаемый формат экспорта: {export_format}")


def _source_factor(source: str) -> float:
    if source == "Copernicus":
        return 1.0
    if source == "NASA":
        return 1.2
    return 0.9


def _metric_value(source: str, metric: str, ts: datetime) -> float:
    factor = _source_factor(source)
    hour = ts.hour
    day = ts.day

    if metric == "precipitation":
        return round(max(0.0, (hour % 6) * 0.4 * factor + (day % 3) * 0.2), 4)
    if metric == "temperature":
        return round(8.0 + (hour % 24) * 0.6 * factor + (day % 5) * 0.3, 4)
    if metric == "wind_speed":
        return round(1.5 + (hour % 8) * 0.5 * factor, 4)
    if metric == "cloudiness":
        return round(min(100.0, 25.0 + (hour % 12) * 6.5 * factor), 4)
    if metric == "ndvi":
        base = 0.35 + (day % 10) * 0.015 * factor
        return round(min(0.95, base), 4)
    raise Stage3Error(f"Неизвестная метрика: {metric}")


def _quality_flags(source: str, metric: str, ts: datetime) -> list[str]:
    flags: list[str] = []

    if metric == "ndvi" and ts.hour % 11 == 0:
        flags.append("cloudy")
    if source == "Mock" and ts.hour % 17 == 0:
        flags.append("simulated")
    if metric == "wind_speed" and ts.hour % 13 == 0:
        flags.append("low_confidence")

    return flags


def _ensure_default_field(db: DbClient) -> int:
    db.exec_checked(
        """
        INSERT INTO enterprises (name)
        SELECT 'ООО Демонстрационное хозяйство'
        WHERE NOT EXISTS (SELECT 1 FROM enterprises);
        """
    )

    db.exec_checked(
        """
        INSERT INTO fields (enterprise_id, season_id, name, geom)
        SELECT
            (SELECT id FROM enterprises ORDER BY id LIMIT 1),
            NULL,
            'Демо-поле',
            ST_SetSRID(
                ST_GeomFromText('POLYGON((30.3000 59.9000,30.3200 59.9000,30.3200 59.9100,30.3000 59.9100,30.3000 59.9000))'),
                4326
            )
        WHERE NOT EXISTS (SELECT 1 FROM fields);
        """
    )

    field_id_raw = db.exec_checked("SELECT id FROM fields ORDER BY id LIMIT 1;", tuples_only=True)
    if not field_id_raw:
        raise Stage3Error("Не удалось определить поле для синхронизации")
    return int(field_id_raw)


def _upsert_sync_error(db: DbClient, source: str, error: str) -> None:
    db.exec_checked(
        f"""
        INSERT INTO provider_sync_status (source, last_sync_at, last_success_at, status, last_error, updated_at)
        VALUES ({_sql_quote(source)}, NOW(), NULL, 'error', {_sql_quote(error[:1000])}, NOW())
        ON CONFLICT (source)
        DO UPDATE SET
            last_sync_at = NOW(),
            status = 'error',
            last_error = EXCLUDED.last_error,
            updated_at = NOW();
        """
    )


def _cleanup_old_observations(db: DbClient, retention_days: int) -> int:
    deleted_raw = db.exec_checked(
        f"""
        WITH deleted AS (
            DELETE FROM provider_observations
            WHERE observed_at < (NOW() - INTERVAL '{int(retention_days)} days')
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted;
        """,
        tuples_only=True,
    )
    return int(deleted_raw or "0")


def run_sync(db: DbClient, source: str, hours: int, field_id: int | None, retention_days: int) -> dict[str, Any]:
    _validate_source(source)
    if hours < 1:
        raise Stage3Error("Параметр hours должен быть > 0")

    resolved_field_id = field_id if field_id is not None else _ensure_default_field(db)
    end = _to_utc(datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=hours - 1)

    inserted = 0
    value_rows: list[str] = []

    try:
        moment = start
        while moment <= end:
            for metric, (_, unit) in METRICS.items():
                value = _metric_value(source, metric, moment)
                flags = _quality_flags(source, metric, moment)
                meta = {
                    "contract_version": CONTRACT_VERSION,
                    "algorithm_version": "provider.v1",
                    "aggregation": "raw",
                    "srid": 4326,
                }

                value_rows.append(
                    "("
                    f"{resolved_field_id},"
                    f"{_sql_quote(metric)},"
                    f"{value},"
                    f"{_sql_quote(unit)},"
                    f"{_sql_quote(_iso_utc(moment))}::timestamptz,"
                    f"{_sql_quote(source)},"
                    f"{_sql_quote(json.dumps(flags, ensure_ascii=False))}::jsonb,"
                    f"{_sql_quote(json.dumps(meta, ensure_ascii=False))}::jsonb,"
                    "NOW()"
                    ")"
                )
                inserted += 1

            moment += timedelta(hours=1)

        if value_rows:
            db.exec_checked(
                """
                INSERT INTO provider_observations (
                    field_id,
                    metric_code,
                    value,
                    unit,
                    observed_at,
                    source,
                    quality_flags,
                    meta,
                    synced_at
                )
                VALUES
                """
                + ",\n".join(value_rows)
                + """
                ON CONFLICT (field_id, metric_code, observed_at, source)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = EXCLUDED.unit,
                    quality_flags = EXCLUDED.quality_flags,
                    meta = EXCLUDED.meta,
                    synced_at = NOW();
                """
            )

        db.exec_checked(
            f"""
            INSERT INTO provider_sync_status (source, last_sync_at, last_success_at, status, last_error, updated_at)
            VALUES ({_sql_quote(source)}, NOW(), NOW(), 'ok', NULL, NOW())
            ON CONFLICT (source)
            DO UPDATE SET
                last_sync_at = NOW(),
                last_success_at = NOW(),
                status = 'ok',
                last_error = NULL,
                updated_at = NOW();
            """
        )
    except Stage3Error as exc:
        _upsert_sync_error(db, source, str(exc))
        raise

    deleted = _cleanup_old_observations(db, retention_days)

    return {
        "source": source,
        "field_id": resolved_field_id,
        "inserted_points": inserted,
        "range": {
            "from": _iso_utc(start),
            "to": _iso_utc(end),
            "hours": hours,
        },
        "retention": {
            "days": retention_days,
            "deleted_old_points": deleted,
        },
        "last_sync_at": _iso_utc(datetime.now(timezone.utc)),
    }


def get_sync_status(db: DbClient, source: str) -> dict[str, Any]:
    _validate_source(source)

    payload = db.query_json(
        f"""
        SELECT COALESCE((
            SELECT row_to_json(s)
            FROM (
                SELECT
                    source,
                    status,
                    CASE
                        WHEN last_sync_at IS NULL THEN NULL
                        ELSE to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_sync_at,
                    CASE
                        WHEN last_success_at IS NULL THEN NULL
                        ELSE to_char(last_success_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_success_at,
                    last_error,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM provider_sync_status
                WHERE source = {_sql_quote(source)}
            ) s
        ), '{{"source": "{source}", "status": "never"}}'::json);
        """
    )

    assert isinstance(payload, dict)
    return payload


def _next_level(granularity: str) -> str:
    if granularity == "month":
        return "day"
    if granularity == "day":
        return "hour"
    if granularity == "hour":
        return "point"
    return "point"


def query_range(
    db: DbClient,
    *,
    source: str,
    field_id: int,
    range_start: datetime,
    range_end: datetime,
    granularity: str,
) -> dict[str, Any]:
    _validate_source(source)
    _validate_granularity(granularity)

    if range_end < range_start:
        raise Stage3Error("Конец диапазона меньше начала")

    start_iso = _iso_utc(range_start)
    end_iso = _iso_utc(range_end)

    where_clause = (
        f"field_id = {int(field_id)} "
        f"AND source = {_sql_quote(source)} "
        f"AND observed_at BETWEEN {_sql_quote(start_iso)}::timestamptz "
        f"AND {_sql_quote(end_iso)}::timestamptz"
    )

    summary = db.query_json(
        f"""
        SELECT COALESCE(json_agg(row_to_json(s) ORDER BY s.metric), '[]'::json)
        FROM (
            SELECT
                metric_code AS metric,
                MIN(unit) AS unit,
                COUNT(*)::int AS count,
                ROUND(MIN(value)::numeric, 4) AS min,
                ROUND(MAX(value)::numeric, 4) AS max,
                ROUND(AVG(value)::numeric, 4) AS avg
            FROM provider_observations
            WHERE {where_clause}
            GROUP BY metric_code
        ) s;
        """
    )

    if granularity == "point":
        bucket_expr = "to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:00:00\"Z\"')"
    else:
        bucket_expr = (
            "to_char(date_trunc("
            f"'{granularity}', observed_at AT TIME ZONE 'UTC'), "
            "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
        )

    time_bins = db.query_json(
        f"""
        SELECT COALESCE(json_agg(row_to_json(b) ORDER BY b.bucket_start DESC), '[]'::json)
        FROM (
            SELECT
                {bucket_expr} AS bucket_start,
                COUNT(*)::int AS points
            FROM provider_observations
            WHERE {where_clause}
            GROUP BY 1
        ) b;
        """
    )

    records = db.query_json(
        f"""
        SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.timestamp DESC, r.metric), '[]'::json)
        FROM (
            SELECT
                ROUND(value::numeric, 4)::double precision AS value,
                unit,
                to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                source,
                quality_flags,
                meta,
                metric_code AS metric
            FROM provider_observations
            WHERE {where_clause}
        ) r;
        """
    )

    assert isinstance(summary, list)
    assert isinstance(time_bins, list)
    assert isinstance(records, list)

    return {
        "contract_version": CONTRACT_VERSION,
        "query": {
            "field_id": int(field_id),
            "source": source,
            "from": start_iso,
            "to": end_iso,
            "granularity": granularity,
            "next_level": _next_level(granularity),
            "order": "desc",
        },
        "last_sync": get_sync_status(db, source),
        "summary": summary,
        "time_bins": time_bins,
        "records": records,
    }


def create_export_task(
    db: DbClient,
    *,
    source: str,
    field_id: int,
    range_start: datetime,
    range_end: datetime,
    granularity: str,
    export_format: str,
) -> dict[str, Any]:
    _validate_source(source)
    _validate_granularity(granularity)
    _validate_export_format(export_format)

    dataset_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)

    db.exec_checked(
        f"""
        INSERT INTO dataset_slices (
            dataset_id,
            field_id,
            source,
            range_start,
            range_end,
            granularity,
            export_format,
            contract_version,
            request_meta,
            export_status,
            created_at,
            expires_at
        )
        VALUES (
            {_sql_quote(dataset_id)},
            {int(field_id)},
            {_sql_quote(source)},
            {_sql_quote(_iso_utc(range_start))}::timestamptz,
            {_sql_quote(_iso_utc(range_end))}::timestamptz,
            {_sql_quote(granularity)},
            {_sql_quote(export_format)},
            {_sql_quote(CONTRACT_VERSION)},
            {_sql_quote(json.dumps({"requested_at": _iso_utc(now)}, ensure_ascii=False))}::jsonb,
            'queued',
            NOW(),
            NOW() + INTERVAL '30 days'
        );
        """
    )

    return get_export_status(db, dataset_id)


def _write_export_file(dataset_id: str, export_format: str, payload: dict[str, Any]) -> str:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    if export_format == "json":
        path = EXPORT_DIR / f"{dataset_id}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path.relative_to(ROOT))

    path = EXPORT_DIR / f"{dataset_id}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["metric", "value", "unit", "timestamp", "source", "quality_flags", "meta"],
        )
        writer.writeheader()
        for row in payload.get("records", []):
            writer.writerow(
                {
                    "metric": row.get("metric"),
                    "value": row.get("value"),
                    "unit": row.get("unit"),
                    "timestamp": row.get("timestamp"),
                    "source": row.get("source"),
                    "quality_flags": json.dumps(row.get("quality_flags", []), ensure_ascii=False),
                    "meta": json.dumps(row.get("meta", {}), ensure_ascii=False),
                }
            )
    return str(path.relative_to(ROOT))


def _list_export_candidates(db: DbClient, dataset_id: str | None) -> list[dict[str, Any]]:
    if dataset_id:
        sql = f"""
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                dataset_id,
                field_id,
                source,
                to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                granularity,
                export_format
            FROM dataset_slices
            WHERE dataset_id = {_sql_quote(dataset_id)}
              AND export_status IN ('queued', 'processing', 'failed')
        ) x;
        """
    else:
        sql = """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                dataset_id,
                field_id,
                source,
                to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                granularity,
                export_format
            FROM dataset_slices
            WHERE export_status IN ('queued', 'processing')
            ORDER BY created_at ASC
        ) x;
        """

    rows = db.query_json(sql)
    assert isinstance(rows, list)
    return rows


def process_exports(db: DbClient, dataset_id: str | None) -> dict[str, Any]:
    tasks = _list_export_candidates(db, dataset_id)
    processed: list[dict[str, Any]] = []

    for task in tasks:
        ds_id = str(task["dataset_id"])
        db.exec_checked(
            f"""
            UPDATE dataset_slices
            SET export_status = 'processing', export_error = NULL
            WHERE dataset_id = {_sql_quote(ds_id)};
            """
        )

        try:
            payload = query_range(
                db,
                source=str(task["source"]),
                field_id=int(task["field_id"]),
                range_start=_parse_ts(str(task["range_start"])),
                range_end=_parse_ts(str(task["range_end"])),
                granularity=str(task["granularity"]),
            )
            payload["dataset_id"] = ds_id

            file_path = _write_export_file(ds_id, str(task["export_format"]), payload)
            db.exec_checked(
                f"""
                UPDATE dataset_slices
                SET export_status = 'ready',
                    export_error = NULL,
                    export_file_path = {_sql_quote(file_path)}
                WHERE dataset_id = {_sql_quote(ds_id)};
                """
            )

            processed.append({"dataset_id": ds_id, "status": "ready", "file": file_path})
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)[:1500]
            db.exec_checked(
                f"""
                UPDATE dataset_slices
                SET export_status = 'failed',
                    export_error = {_sql_quote(error_text)}
                WHERE dataset_id = {_sql_quote(ds_id)};
                """
            )
            processed.append({"dataset_id": ds_id, "status": "failed", "error": error_text})

    return {"processed": processed, "count": len(processed)}


def get_export_status(db: DbClient, dataset_id: str) -> dict[str, Any]:
    payload = db.query_json(
        f"""
        SELECT COALESCE((
            SELECT row_to_json(s)
            FROM (
                SELECT
                    dataset_id,
                    field_id,
                    source,
                    to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                    to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                    granularity,
                    export_format,
                    contract_version,
                    export_status,
                    export_error,
                    export_file_path,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at,
                    to_char(warned_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS warned_at,
                    extended_count
                FROM dataset_slices
                WHERE dataset_id = {_sql_quote(dataset_id)}
            ) s
        ), '{{"dataset_id": "{dataset_id}", "export_status": "not_found"}}'::json);
        """
    )
    assert isinstance(payload, dict)
    return payload


def extend_dataset_ttl(db: DbClient, dataset_id: str, days: int) -> dict[str, Any]:
    if days <= 0:
        raise Stage3Error("Параметр days должен быть > 0")

    db.exec_checked(
        f"""
        UPDATE dataset_slices
        SET expires_at = expires_at + INTERVAL '{int(days)} days',
            warned_at = NULL,
            extended_count = extended_count + 1
        WHERE dataset_id = {_sql_quote(dataset_id)};
        """
    )

    return get_export_status(db, dataset_id)


def run_ttl_check(db: DbClient) -> dict[str, Any]:
    candidates = db.query_json(
        """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                dataset_id,
                to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM dataset_slices
            WHERE expires_at <= (NOW() + INTERVAL '1 day')
              AND warned_at IS NULL
            ORDER BY expires_at ASC
        ) x;
        """
    )
    assert isinstance(candidates, list)

    warned_ids: list[str] = []
    for row in candidates:
        dataset_id = str(row["dataset_id"])
        expires_at = str(row["expires_at"])
        message = (
            "Срок хранения набора данных истекает менее чем через 1 день: "
            f"dataset_id={dataset_id}, expires_at={expires_at}"
        )

        db.exec_checked(
            f"""
            INSERT INTO dataset_notifications (dataset_id, channel, message)
            VALUES ({_sql_quote(dataset_id)}, 'ui', {_sql_quote(message)});

            UPDATE dataset_slices
            SET warned_at = NOW()
            WHERE dataset_id = {_sql_quote(dataset_id)};
            """
        )
        warned_ids.append(dataset_id)

    return {"warned_count": len(warned_ids), "dataset_ids": warned_ids}


def view_dataset(db: DbClient, dataset_id: str, granularity: str | None) -> dict[str, Any]:
    dataset = db.query_json(
        f"""
        SELECT COALESCE((
            SELECT row_to_json(x)
            FROM (
                SELECT
                    dataset_id,
                    field_id,
                    source,
                    to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                    to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                    granularity,
                    export_format,
                    contract_version,
                    export_status,
                    export_file_path,
                    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
                FROM dataset_slices
                WHERE dataset_id = {_sql_quote(dataset_id)}
            ) x
        ), '{{}}'::json);
        """
    )
    if not isinstance(dataset, dict) or not dataset:
        raise Stage3Error(f"Набор данных {dataset_id} не найден")

    selected_granularity = granularity or str(dataset["granularity"])
    _validate_granularity(selected_granularity)

    payload = query_range(
        db,
        source=str(dataset["source"]),
        field_id=int(dataset["field_id"]),
        range_start=_parse_ts(str(dataset["range_start"])),
        range_end=_parse_ts(str(dataset["range_end"])),
        granularity=selected_granularity,
    )
    payload["dataset"] = dataset

    db.exec_checked(
        f"""
        UPDATE dataset_slices
        SET last_accessed_at = NOW()
        WHERE dataset_id = {_sql_quote(dataset_id)};
        """
    )

    return payload


def set_dataset_expiry_for_test(db: DbClient, dataset_id: str, hours: int) -> dict[str, Any]:
    db.exec_checked(
        f"""
        UPDATE dataset_slices
        SET expires_at = NOW() + INTERVAL '{int(hours)} hours',
            warned_at = NULL
        WHERE dataset_id = {_sql_quote(dataset_id)};
        """
    )
    return get_export_status(db, dataset_id)


def run_cycle(db: DbClient, hours: int, retention_days: int) -> dict[str, Any]:
    sync_reports = [run_sync(db, source, hours, None, retention_days) for source in sorted(VALID_SOURCES)]
    exports = process_exports(db, None)
    ttl = run_ttl_check(db)
    return {"sync": sync_reports, "exports": exports, "ttl": ttl}


def _print_json(payload: dict[str, Any] | list[Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 3: контракт данных и провайдеры")
    sub = parser.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync")
    sync.add_argument("--source", required=True)
    sync.add_argument("--hours", type=int, default=72)
    sync.add_argument("--field-id", type=int)
    sync.add_argument("--retention-days", type=int, default=30)

    sync_status = sub.add_parser("sync-status")
    sync_status.add_argument("--source", required=True)

    query = sub.add_parser("query")
    query.add_argument("--source", required=True)
    query.add_argument("--field-id", required=True, type=int)
    query.add_argument("--from", dest="range_from", required=True)
    query.add_argument("--to", dest="range_to", required=True)
    query.add_argument("--granularity", required=True)

    export_create = sub.add_parser("export-create")
    export_create.add_argument("--source", required=True)
    export_create.add_argument("--field-id", required=True, type=int)
    export_create.add_argument("--from", dest="range_from", required=True)
    export_create.add_argument("--to", dest="range_to", required=True)
    export_create.add_argument("--granularity", required=True)
    export_create.add_argument("--format", dest="export_format", required=True)

    export_process = sub.add_parser("export-process")
    export_process.add_argument("--dataset-id")

    export_status = sub.add_parser("export-status")
    export_status.add_argument("--dataset-id", required=True)

    ttl = sub.add_parser("ttl-check")

    extend = sub.add_parser("dataset-extend")
    extend.add_argument("--dataset-id", required=True)
    extend.add_argument("--days", required=True, type=int)

    view = sub.add_parser("dataset-view")
    view.add_argument("--dataset-id", required=True)
    view.add_argument("--granularity")

    expiry = sub.add_parser("dataset-set-expiry")
    expiry.add_argument("--dataset-id", required=True)
    expiry.add_argument("--hours", required=True, type=int)

    cycle = sub.add_parser("run-cycle")
    cycle.add_argument("--hours", type=int, default=24)
    cycle.add_argument("--retention-days", type=int, default=30)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    db = DbClient()

    try:
        db.ensure_ready()

        if args.command == "sync":
            _print_json(
                run_sync(
                    db,
                    source=args.source,
                    hours=args.hours,
                    field_id=args.field_id,
                    retention_days=args.retention_days,
                )
            )
            return 0

        if args.command == "sync-status":
            _print_json(get_sync_status(db, source=args.source))
            return 0

        if args.command == "query":
            _print_json(
                query_range(
                    db,
                    source=args.source,
                    field_id=args.field_id,
                    range_start=_parse_ts(args.range_from),
                    range_end=_parse_ts(args.range_to),
                    granularity=args.granularity,
                )
            )
            return 0

        if args.command == "export-create":
            _print_json(
                create_export_task(
                    db,
                    source=args.source,
                    field_id=args.field_id,
                    range_start=_parse_ts(args.range_from),
                    range_end=_parse_ts(args.range_to),
                    granularity=args.granularity,
                    export_format=args.export_format,
                )
            )
            return 0

        if args.command == "export-process":
            _print_json(process_exports(db, dataset_id=args.dataset_id))
            return 0

        if args.command == "export-status":
            _print_json(get_export_status(db, dataset_id=args.dataset_id))
            return 0

        if args.command == "ttl-check":
            _print_json(run_ttl_check(db))
            return 0

        if args.command == "dataset-extend":
            _print_json(extend_dataset_ttl(db, dataset_id=args.dataset_id, days=args.days))
            return 0

        if args.command == "dataset-view":
            _print_json(view_dataset(db, dataset_id=args.dataset_id, granularity=args.granularity))
            return 0

        if args.command == "dataset-set-expiry":
            _print_json(set_dataset_expiry_for_test(db, dataset_id=args.dataset_id, hours=args.hours))
            return 0

        if args.command == "run-cycle":
            _print_json(run_cycle(db, hours=args.hours, retention_days=args.retention_days))
            return 0

        raise Stage3Error("Неизвестная команда")
    except Stage3Error as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
