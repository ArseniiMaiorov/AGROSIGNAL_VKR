from __future__ import annotations

import json
import math
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "reports" / "tests" / f"{date.today().isoformat()}_stage6_algorithms.md"
FIXTURES_ALGO = ROOT / "tests" / "fixtures" / "algorithms"
FIXTURES_GEO = ROOT / "tests" / "fixtures" / "geo"

import sys

if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from api.main import create_server  # noqa: E402
from stage3_cli import DbClient, _iso_utc  # noqa: E402


@dataclass
class CaseResult:
    feature: str
    input_data: str
    expected: str
    actual: str
    status: str


def add_case(cases: list[CaseResult], feature: str, input_data: str, expected: str, ok: bool, actual: str) -> None:
    cases.append(
        CaseResult(
            feature=feature,
            input_data=input_data,
            expected=expected,
            actual=actual,
            status="PASS" if ok else "FAIL",
        )
    )


def write_report(cases: list[CaseResult]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["# Протокол тестирования этапа 6: алгоритмы и расчёты (агро-логика)", ""]

    for case in cases:
        lines.extend(
            [
                f"Фича: {case.feature}",
                "Вход:",
                f"  - {case.input_data}",
                "Ожидаемый результат:",
                f"  - {case.expected}",
                "Фактический результат:",
                f"  - {case.actual}",
                f"Статус: {case.status}",
                "",
            ]
        )

    total = len(cases)
    passed = len([case for case in cases if case.status == "PASS"])
    failed = total - passed
    lines.extend(
        [
            "## Сводка",
            f"Всего тестов: {total}",
            f"PASS: {passed}",
            f"FAIL: {failed}",
            "",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def api_request(
    *,
    base_url: str,
    method: str,
    path: str,
    email: str,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    body = b""
    headers = {
        "Content-Type": "application/json",
        "X-User-Email": email,
    }
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=f"{base_url}{path}",
        method=method,
        data=body if body else None,
        headers=headers,
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
            return int(response.status), data, dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        data = json.loads(raw) if raw else {}
        return int(exc.code), data, dict(exc.headers)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    coords: list[tuple[float, float]] = []
    for ring in geometry.get("coordinates", []):
        for point in ring:
            if isinstance(point, list) and len(point) >= 2:
                coords.append((float(point[0]), float(point[1])))
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lons), min(lats), max(lons), max(lats)


def assert_close(a: float, b: float, tolerance: float) -> bool:
    return abs(a - b) <= tolerance


def parse_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def shift_fixture_into_retention_window(
    fixture: dict[str, Any],
    *,
    retention_days: int = 30,
    safety_hours: int = 2,
) -> dict[str, Any]:
    shifted = deepcopy(fixture)
    source_from = parse_utc(str(shifted["from"]))
    source_to = parse_utc(str(shifted["to"]))
    now = datetime.now(timezone.utc)
    max_allowed = now - timedelta(hours=safety_hours)
    min_allowed = now - timedelta(days=retention_days - 1)

    if source_from >= min_allowed and source_to <= max_allowed:
        return shifted

    # Keep original hour boundaries from fixture (golden per-day values rely on this).
    delta_days = (max_allowed.date() - source_to.date()).days
    delta = timedelta(days=delta_days)
    shifted_from = source_from + delta
    shifted_to = source_to + delta

    while shifted_to > max_allowed:
        delta -= timedelta(days=1)
        shifted_from = source_from + delta
        shifted_to = source_to + delta
    while shifted_from < min_allowed:
        delta += timedelta(days=1)
        shifted_from = source_from + delta
        shifted_to = source_to + delta

    if shifted_from < min_allowed or shifted_to > max_allowed:
        # Fallback for narrow windows.
        duration = source_to - source_from
        target_to = max_allowed.replace(minute=0, second=0, microsecond=0)
        target_from = target_to - duration
        delta = target_from - source_from

    shifted["from"] = _iso_utc(source_from + delta)
    shifted["to"] = _iso_utc(source_to + delta)
    records = shifted.get("records", [])
    if isinstance(records, list):
        for row in records:
            if not isinstance(row, dict):
                continue
            timestamp_raw = row.get("timestamp")
            if not isinstance(timestamp_raw, str):
                continue
            row["timestamp"] = _iso_utc(parse_utc(timestamp_raw) + delta)
    return shifted


def seed_algorithm_dataset(field_id: int, source: str, fixture: dict[str, Any]) -> None:
    db = DbClient()
    db.ensure_ready()

    db.exec_checked(
        f"""
        INSERT INTO provider_sync_status (source, last_sync_at, last_success_at, status, last_error, updated_at)
        VALUES ('{source}', NOW(), NOW(), 'ok', NULL, NOW())
        ON CONFLICT (source)
        DO UPDATE SET last_sync_at = NOW(), last_success_at = NOW(), status = 'ok', last_error = NULL, updated_at = NOW();
        """
    )

    metric_units = {
        "temperature": "C",
        "humidity_rh": "%",
        "wind_speed": "m/s",
        "radiation": "W/m2",
        "precipitation": "mm",
        "soil_moisture": "%",
        "ndvi": "index",
        "ndre": "index",
        "ndmi": "index",
        "cloudiness": "%",
        "cloud_total": "%",
        "cloud_mask": "%",
    }

    for idx, row in enumerate(fixture.get("records", [])):
        if not isinstance(row, dict):
            continue
        ts = str(row.get("timestamp"))
        metrics = row.get("metrics", {})
        if not isinstance(metrics, dict):
            continue
        for metric, value in metrics.items():
            unit = metric_units.get(metric, "")
            flags = []
            if metric in {"cloud_total", "cloud_mask"} and idx in {2, 6}:
                flags = ["low_quality"]
            db.exec_checked(
                f"""
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
                ) VALUES (
                    {field_id},
                    '{metric}',
                    {float(value)},
                    '{unit}',
                    '{ts}'::timestamptz,
                    '{source}',
                    '{json.dumps(flags, ensure_ascii=False)}'::jsonb,
                    '{{"contract_version":"v1.0-stage6-test"}}'::jsonb,
                    NOW()
                )
                ON CONFLICT (field_id, metric_code, observed_at, source)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = EXCLUDED.unit,
                    quality_flags = EXCLUDED.quality_flags,
                    meta = EXCLUDED.meta,
                    synced_at = NOW();
                """
            )


def main() -> int:
    cases: list[CaseResult] = []
    fixture = shift_fixture_into_retention_window(load_json(FIXTURES_ALGO / "baseline_input.json"))
    expected_gdd = load_json(FIXTURES_ALGO / "gdd_expected.json")
    expected_vpd = load_json(FIXTURES_ALGO / "vpd_expected.json")
    expected_et0 = load_json(FIXTURES_ALGO / "et0_expected.json")
    expected_wd = load_json(FIXTURES_ALGO / "water_deficit_expected.json")

    from_ts = str(fixture["from"])
    to_ts = str(fixture["to"])
    source = str(fixture["source"])

    server = create_server(host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"

    manager_email = "manager@zemledar.local"
    field_id = 0
    bbox = ""
    center_lon = 0.0
    center_lat = 0.0

    try:
        # 0) Create dedicated field
        geometry = load_json(FIXTURES_GEO / "field_ok.geojson")["geometry"]
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/fields",
            email=manager_email,
            payload={
                "enterprise_id": 1,
                "name": f"Поле Этап6 {uuid.uuid4().hex[:6]}",
                "geojson": geometry,
                "srid": 4326,
            },
        )
        field_id = int(payload.get("data", {}).get("id") or 0)
        min_lon, min_lat, max_lon, max_lat = geometry_bbox(geometry)
        bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"
        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        add_case(
            cases,
            feature="Подготовка поля для stage6",
            input_data="POST /api/v1/fields",
            expected="Поле создано",
            ok=status == 201 and field_id > 0,
            actual=f"status={status}, field_id={field_id}",
        )
        if field_id <= 0:
            raise RuntimeError("Не удалось создать поле для stage6")

        seed_algorithm_dataset(field_id, source, fixture)

        # 1) GDD golden fixture
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/gdd?source={source}&from={from_ts}&to={to_ts}&tbase={fixture['tbase_c']}",
            email=manager_email,
        )
        derived = payload.get("data", {}).get("derived", {}) if isinstance(payload.get("data"), dict) else {}
        values = derived.get("values", []) if isinstance(derived, dict) else []
        tolerance = float(expected_gdd.get("tolerance", 0.001))
        ok = status == 200 and derived.get("status") == "OK" and len(values) == len(expected_gdd["values"])
        if ok:
            for got, exp in zip(values, expected_gdd["values"]):
                ok = ok and bool(str(got.get("date") or ""))
                ok = ok and assert_close(float(got.get("gdd_day") or 0.0), float(exp["gdd_day"]), tolerance)
                ok = ok and assert_close(float(got.get("gdd_accum") or 0.0), float(exp["gdd_accum"]), tolerance)
        add_case(
            cases,
            feature="Golden fixture: GDD",
            input_data="GET /fields/{id}/algorithms/gdd",
            expected="status=OK и значения в пределах допусков",
            ok=ok,
            actual=f"status={status}, derived_status={derived.get('status')}, values={values}",
        )

        # 2) VPD golden fixture
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/vpd?source={source}&from={from_ts}&to={to_ts}&granularity=day",
            email=manager_email,
        )
        derived = payload.get("data", {}).get("derived", {}) if isinstance(payload.get("data"), dict) else {}
        values = derived.get("values", []) if isinstance(derived, dict) else []
        tolerance = float(expected_vpd.get("tolerance", 0.001))
        ok = status == 200 and derived.get("status") == "OK" and len(values) == len(expected_vpd["values"])
        if ok:
            for got, exp in zip(values, expected_vpd["values"]):
                ok = ok and bool(str(got.get("date") or ""))
                ok = ok and assert_close(float(got.get("vpd_kpa") or 0.0), float(exp["vpd_kpa"]), tolerance)
        add_case(
            cases,
            feature="Golden fixture: VPD",
            input_data="GET /fields/{id}/algorithms/vpd?granularity=day",
            expected="status=OK и значения VPD в пределах допусков",
            ok=ok,
            actual=f"status={status}, derived_status={derived.get('status')}, values={values}",
        )

        # 3) ET0 golden fixture
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/et0?source={source}&from={from_ts}&to={to_ts}&granularity=day&allow_approx=true",
            email=manager_email,
        )
        derived = payload.get("data", {}).get("derived", {}) if isinstance(payload.get("data"), dict) else {}
        values = derived.get("values", []) if isinstance(derived, dict) else []
        tolerance = float(expected_et0.get("tolerance", 0.001))
        ok = status == 200 and derived.get("status") == "OK" and len(values) == len(expected_et0["values"])
        if ok:
            for got, exp in zip(values, expected_et0["values"]):
                ok = ok and bool(str(got.get("date") or ""))
                ok = ok and assert_close(float(got.get("et0_mm_day") or 0.0), float(exp["et0_mm_day"]), tolerance)
        add_case(
            cases,
            feature="Golden fixture: ET0",
            input_data="GET /fields/{id}/algorithms/et0?granularity=day",
            expected="status=OK, корректный variant и значения в пределах допусков",
            ok=ok,
            actual=f"status={status}, variant={derived.get('algorithm_variant')}, values={values}",
        )

        # 4) Water deficit golden fixture
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/water-deficit?source={source}&from={from_ts}&to={to_ts}",
            email=manager_email,
        )
        derived = payload.get("data", {}).get("derived", {}) if isinstance(payload.get("data"), dict) else {}
        values = derived.get("values", []) if isinstance(derived, dict) else []
        tolerance = float(expected_wd.get("tolerance", 0.001))
        ok = status == 200 and derived.get("status") == "OK" and len(values) == 1
        if ok:
            got = values[0]
            exp = expected_wd["values"][0]
            ok = ok and assert_close(float(got.get("et0_sum_mm") or 0.0), float(exp["et0_sum_mm"]), tolerance)
            ok = ok and assert_close(float(got.get("precip_sum_mm") or 0.0), float(exp["precip_sum_mm"]), tolerance)
            ok = ok and assert_close(float(got.get("water_deficit_mm") or 0.0), float(exp["water_deficit_mm"]), tolerance)
        add_case(
            cases,
            feature="Golden fixture: Water deficit",
            input_data="GET /fields/{id}/algorithms/water-deficit",
            expected="status=OK и корректная дельта ET0_sum - precip_sum",
            ok=ok,
            actual=f"status={status}, derived={derived}",
        )

        # 5) Zoom detail rules
        zoom_expectations = {9: 1000, 11: 500, 13: 250, 15: 100}
        zoom_results: list[str] = []
        zoom_ok = True
        for zoom, expected_cell_size in zoom_expectations.items():
            status, payload, _ = api_request(
                base_url=base_url,
                method="GET",
                path=(
                    f"/api/v1/layers/weather.wind_speed_10m/grid?source={source}&bbox={bbox}"
                    f"&zoom={zoom}&from={from_ts}&to={to_ts}&granularity=hour&agg=mean&field_id={field_id}"
                ),
                email=manager_email,
            )
            data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
            got_size = data.get("grid", {}).get("cell_size_m") if isinstance(data.get("grid"), dict) else None
            zoom_results.append(f"z{zoom}:{got_size}")
            zoom_ok = zoom_ok and status == 200 and int(got_size or -1) == expected_cell_size
        add_case(
            cases,
            feature="Пространственные тесты: zoom-детализация",
            input_data="GET /layers/{layer_id}/grid на zoom 9/11/13/15",
            expected="cell_size_m = 1000/500/250/100 + meta режима",
            ok=zoom_ok,
            actual=", ".join(zoom_results),
        )

        # 6) Probe point
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=(
                f"/api/v1/fields/{field_id}/probe?source={source}&lat={center_lat}&lon={center_lon}"
                f"&time={to_ts}&layers=weather.wind_speed_10m,sat.ndvi"
            ),
            email=manager_email,
        )
        data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        values = data.get("values", []) if isinstance(data, dict) else []
        ok = status == 200 and len(values) >= 1 and isinstance(data.get("mini_stats"), dict)
        add_case(
            cases,
            feature="Пространственные тесты: probe",
            input_data="GET /fields/{id}/probe",
            expected="Возвращается значение для запрошенных слоёв + mini_stats/mini_reco",
            ok=ok,
            actual=f"status={status}, values={len(values)}, mini_stats={data.get('mini_stats')}",
        )

        # 7) Zones + zonal stats
        status_z, payload_z, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/zones?source={source}&zoom=15&time={to_ts}&method=grid",
            email=manager_email,
        )
        zones = payload_z.get("data", {}).get("zones", []) if isinstance(payload_z.get("data"), dict) else []
        zone_id = str(zones[0].get("zone_id")) if zones else ""
        status_s, payload_s, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/zonal-stats?source={source}&zone_id={zone_id}&from={from_ts}&to={to_ts}&metrics=ndvi,temperature",
            email=manager_email,
        )
        items = payload_s.get("data", {}).get("items", []) if isinstance(payload_s.get("data"), dict) else []
        ok = status_z == 200 and len(zones) > 0 and status_s == 200 and len(items) > 0
        add_case(
            cases,
            feature="Пространственные тесты: zones/zonal-stats",
            input_data="GET /zones + GET /zonal-stats",
            expected="Зоны формируются, статистика по зонам доступна",
            ok=ok,
            actual=f"zones={len(zones)}, zonal_items={len(items)}",
        )

        # 8) Negative: INSUFFICIENT_DATA
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/gdd?source={source}&from=1999-01-01T00:00:00Z&to=1999-01-02T00:00:00Z&tbase=10",
            email=manager_email,
        )
        derived = payload.get("data", {}).get("derived", {}) if isinstance(payload.get("data"), dict) else {}
        ok = status == 200 and derived.get("status") == "INSUFFICIENT_DATA"
        add_case(
            cases,
            feature="Негативные тесты: INSUFFICIENT_DATA",
            input_data="GET /algorithms/gdd на пустом диапазоне",
            expected="status=INSUFFICIENT_DATA + причина",
            ok=ok,
            actual=f"status={status}, derived={derived}",
        )

        # 9) Negative: VALIDATION_ERROR (unit mismatch)
        db = DbClient()
        bad_ts = "2026-01-12T00:00:00Z"
        db.exec_checked(
            f"""
            INSERT INTO provider_observations (
                field_id, metric_code, value, unit, observed_at, source, quality_flags, meta, synced_at
            ) VALUES (
                {field_id}, 'temperature', 300.15, 'K', '{bad_ts}'::timestamptz, '{source}',
                '[]'::jsonb, '{{\"contract_version\":\"v1.0-stage6-test\"}}'::jsonb, NOW()
            )
            ON CONFLICT (field_id, metric_code, observed_at, source)
            DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit;
            """
        )
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/algorithms/gdd?source={source}&from={bad_ts}&to={bad_ts}&tbase=10",
            email=manager_email,
        )
        err = payload.get("error", {}) if isinstance(payload.get("error"), dict) else {}
        ok = status == 422 and err.get("code") == "VALIDATION_ERROR"
        add_case(
            cases,
            feature="Негативные тесты: VALIDATION_ERROR (единицы)",
            input_data="GET /algorithms/gdd при unit=K",
            expected="422 VALIDATION_ERROR",
            ok=ok,
            actual=f"status={status}, error={err}",
        )

        # 10) Negative: LOW_QUALITY mark (data remains visible)
        status, payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/satellite/quality?source={source}&from={from_ts}&to={to_ts}",
            email=manager_email,
        )
        quality_items = payload.get("data", {}).get("quality", []) if isinstance(payload.get("data"), dict) else []
        has_low_quality = any(str(item.get("quality_level")) == "LOW_QUALITY" for item in quality_items if isinstance(item, dict))
        ok = status == 200 and len(quality_items) > 0 and has_low_quality
        add_case(
            cases,
            feature="Негативные тесты: LOW_QUALITY",
            input_data="GET /satellite/quality с флагом low_quality",
            expected="Данные остаются видимыми, но помечаются LOW_QUALITY",
            ok=ok,
            actual=f"status={status}, quality_items={len(quality_items)}, has_low_quality={has_low_quality}",
        )

        # 11) Modeling: baseline vs scenario
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/modeling/scenarios",
            email=manager_email,
            payload={
                "field_id": field_id,
                "source": source,
                "from": from_ts,
                "to": to_ts,
                "params": {"rain_delta_mm": 10, "temp_shift_c": 1.5},
            },
        )
        scenario_id = str(payload.get("data", {}).get("scenario_id") or "")
        run_status, _, _ = api_request(
            base_url=base_url,
            method="POST",
            path=f"/api/v1/modeling/scenarios/{scenario_id}/run",
            email=manager_email,
            payload={},
        )
        diff_status, diff_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/modeling/scenarios/{scenario_id}/diff",
            email=manager_email,
        )
        diff_obj = diff_payload.get("data", {}).get("diff", {}) if isinstance(diff_payload.get("data"), dict) else {}
        metrics = diff_obj.get("metrics", []) if isinstance(diff_obj, dict) else []
        derived_metrics = diff_obj.get("derived_metrics", []) if isinstance(diff_obj, dict) else []
        has_precip_delta = any(str(m.get("metric")) == "precipitation" and float(m.get("delta") or 0) > 0 for m in metrics if isinstance(m, dict))
        has_water_diff = any(str(m.get("metric")) == "water_deficit_mm" for m in derived_metrics if isinstance(m, dict))
        ok = status == 201 and bool(scenario_id) and run_status == 202 and diff_status == 200 and has_precip_delta and has_water_diff
        add_case(
            cases,
            feature="Моделирование: baseline vs scenario",
            input_data="POST scenario + POST run + GET diff",
            expected="Есть дельта по водным метрикам и пересчёт рекомендаций/derived",
            ok=ok,
            actual=f"create={status}, run={run_status}, diff={diff_status}, has_precip_delta={has_precip_delta}, has_water_diff={has_water_diff}",
        )

        # 12) Modeling negative: out-of-range window
        out_from = _iso_utc(datetime.now(timezone.utc) - timedelta(days=90))
        out_to = _iso_utc(datetime.now(timezone.utc) - timedelta(days=89))
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/modeling/scenarios",
            email=manager_email,
            payload={
                "field_id": field_id,
                "source": source,
                "from": out_from,
                "to": out_to,
                "params": {"rain_delta_mm": 5},
            },
        )
        err = payload.get("error", {}) if isinstance(payload.get("error"), dict) else {}
        ok = status == 422 and err.get("code") == "VALIDATION_ERROR"
        add_case(
            cases,
            feature="Моделирование: сценарий вне диапазона",
            input_data="POST /modeling/scenarios с from за пределами retention",
            expected="422 VALIDATION_ERROR",
            ok=ok,
            actual=f"status={status}, error={err}",
        )

        # 13) Modeling negative: scenario without baseline
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/modeling/scenarios",
            email=manager_email,
            payload={
                "field_id": field_id,
                "source": source,
                "from": from_ts,
                "to": to_ts,
                "baseline_id": "",
                "params": {"rain_delta_mm": 3},
            },
        )
        err = payload.get("error", {}) if isinstance(payload.get("error"), dict) else {}
        ok = status == 422 and err.get("code") == "VALIDATION_ERROR"
        add_case(
            cases,
            feature="Моделирование: сценарий без baseline",
            input_data="POST /modeling/scenarios baseline_id=''",
            expected="422 VALIDATION_ERROR",
            ok=ok,
            actual=f"status={status}, error={err}",
        )

    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)

    write_report(cases)
    print(f"Сформирован отчёт: {REPORT_PATH.relative_to(ROOT)}")

    failed = [case for case in cases if case.status == "FAIL"]
    if failed:
        for case in failed:
            print(f"FAIL: {case.feature} -> {case.actual}")
        return 1

    for case in cases:
        print(f"PASS: {case.feature}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
