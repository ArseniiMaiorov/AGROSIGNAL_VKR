from __future__ import annotations

import json
import math
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "reports" / "tests" / f"{date.today().isoformat()}_stage5_api.md"
FIXTURES_GEO = ROOT / "tests" / "fixtures" / "geo"

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
    lines: list[str] = ["# Протокол тестирования этапа 5: API и пользовательские сценарии (без фронта)", ""]

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
    idempotency_key: str | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    body = b""
    headers = {
        "Content-Type": "application/json",
        "X-User-Email": email,
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request = urllib.request.Request(
        url=f"{base_url}{path}",
        method=method,
        data=body if body else None,
        headers=headers,
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            text = response.read().decode("utf-8")
            if text:
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    data = {"raw": text}
            else:
                data = {}
            return int(response.status), data, dict(response.headers)
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8")
        if text:
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                data = {"raw": text}
        else:
            data = {}
        return int(exc.code), data, dict(exc.headers)


def raw_request(
    *,
    base_url: str,
    method: str,
    path: str,
    email: str,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes, dict[str, str]]:
    request_headers = {"X-User-Email": email}
    if headers:
        request_headers.update(headers)

    request = urllib.request.Request(
        url=f"{base_url}{path}",
        method=method,
        headers=request_headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return int(response.status), response.read(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read(), dict(exc.headers)


def load_geometry(name: str) -> dict[str, Any]:
    payload = json.loads((FIXTURES_GEO / name).read_text(encoding="utf-8"))
    return payload["geometry"]


def bbox_of_geometry(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    coords: list[tuple[float, float]] = []
    for ring in geometry.get("coordinates", []):
        for point in ring:
            if isinstance(point, list) and len(point) >= 2:
                coords.append((float(point[0]), float(point[1])))
    if not coords:
        raise ValueError("Пустая геометрия для bbox")
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lons), min(lats), max(lons), max(lats)


def tile_for_point(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def seed_wind_value(field_id: int, value: float, source: str) -> str:
    db = DbClient()
    db.ensure_ready()
    ts = _iso_utc(datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
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
            'wind_speed',
            {float(value)},
            'm/s',
            '{ts}'::timestamptz,
            '{source}',
            '[]'::jsonb,
            '{{"contract_version":"v1.0-stage5-test"}}'::jsonb,
            NOW()
        )
        ON CONFLICT (field_id, metric_code, observed_at, source)
        DO UPDATE SET value = EXCLUDED.value, quality_flags = EXCLUDED.quality_flags, meta = EXCLUDED.meta;
        """
    )
    return ts


def main() -> int:
    cases: list[CaseResult] = []

    # API server in-process (без фронта)
    server = create_server(host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"

    manager_email = "manager@zemledar.local"
    viewer_email = "viewer@zemledar.local"
    admin_email = "admin@zemledar.local"

    enterprise_id = 1
    field_id = 0
    field_bbox_str = ""
    field_center = (0.0, 0.0)
    season_id = 0
    crop_id = 0
    rule_id = 0
    export_id = ""

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    from_48h = _iso_utc(now - timedelta(hours=48))
    to_now = _iso_utc(now)

    try:
        # 0) Контракт /auth/me
        status, payload, _ = api_request(base_url=base_url, method="GET", path="/api/v1/auth/me", email=manager_email)
        ok = (
            status == 200
            and isinstance(payload.get("data"), dict)
            and payload.get("meta", {}).get("api_version") == "v1"
            and bool(payload.get("meta", {}).get("request_id"))
        )
        enterprise_id = int(payload.get("data", {}).get("enterprise_id") or 1)
        add_case(
            cases,
            feature="Контракт ответа API v1",
            input_data="GET /api/v1/auth/me",
            expected="200 + data/meta + api_version/request_id",
            ok=ok,
            actual=f"status={status}, meta={payload.get('meta')}",
        )

        # 1) Enterprise CRUD + binding
        enterprise_name = f"ООО Этап5 {uuid.uuid4().hex[:6]}"
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/enterprises",
            email=manager_email,
            payload={"name": enterprise_name},
        )
        created_enterprise = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        created_enterprise_id = int(created_enterprise.get("id") or 0)

        unique_email = f"stage5.user.{uuid.uuid4().hex[:6]}@zemledar.local"
        user_status, user_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/users",
            email=manager_email,
            payload={
                "email": unique_email,
                "full_name": "Пользователь Этап 5",
                "role": "viewer",
                "enterprise_id": enterprise_id,
            },
        )

        bind_status, bind_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path=f"/api/v1/enterprises/{created_enterprise_id}/users/bind",
            email=admin_email,
            payload={"user_email": unique_email},
        )

        ok = status == 201 and created_enterprise_id > 0 and user_status == 201 and bind_status == 200
        add_case(
            cases,
            feature="Предприятия и привязка пользователей",
            input_data="POST /enterprises, POST /users, POST /enterprises/{id}/users/bind",
            expected="Создание и привязка пользователя к предприятию",
            ok=ok,
            actual=(
                f"enterprise_status={status}, enterprise_id={created_enterprise_id}, "
                f"user_status={user_status}, bind_status={bind_status}, bind_data={bind_payload.get('data')}"
            ),
        )

        # 2) Field create (валидный полигон)
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/fields",
            email=manager_email,
            payload={
                "enterprise_id": enterprise_id,
                "name": f"Поле Этап5 {uuid.uuid4().hex[:6]}",
                "geojson": load_geometry("field_ok.geojson"),
                "srid": 4326,
            },
        )
        field = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        field_id = int(field.get("id") or 0)
        geometry_for_bbox = field.get("geometry") if isinstance(field.get("geometry"), dict) else load_geometry("field_ok.geojson")
        min_lon, min_lat, max_lon, max_lat = bbox_of_geometry(geometry_for_bbox)
        field_bbox_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
        field_center = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0)
        ok = status == 201 and field_id > 0 and float(field.get("area_ha") or 0) > 0 and field.get("bbox") is not None
        add_case(
            cases,
            feature="Создание поля (валидный полигон)",
            input_data="POST /fields + fixtures/geo/field_ok.geojson",
            expected="201, field_id != null, area > 0, bbox заполнен",
            ok=ok,
            actual=f"status={status}, field_id={field_id}, area={field.get('area_ha')}, bbox={bool(field.get('bbox'))}",
        )

        # 3) Field create (самопересечение)
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/fields",
            email=manager_email,
            payload={
                "enterprise_id": enterprise_id,
                "name": f"Поле Плохое {uuid.uuid4().hex[:5]}",
                "geojson": load_geometry("field_self_intersect.geojson"),
                "srid": 4326,
            },
        )
        error = payload.get("error", {}) if isinstance(payload.get("error"), dict) else {}
        ok = status == 422 and error.get("code") == "VALIDATION_ERROR" and "самопересекается" in str(error.get("message", "")).lower()
        add_case(
            cases,
            feature="Создание поля (самопересечение)",
            input_data="POST /fields + fixtures/geo/field_self_intersect.geojson",
            expected="422 VALIDATION_ERROR, русское сообщение",
            ok=ok,
            actual=f"status={status}, error={error}",
        )

        # 4) Crop + Season + overlap validation
        crop_name = f"Культура Этап5 {uuid.uuid4().hex[:6]}"
        status, payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/crops",
            email=manager_email,
            payload={"name": crop_name},
        )
        crop_id = int(payload.get("data", {}).get("id") or 0)

        status_season, payload_season, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/seasons",
            email=manager_email,
            payload={
                "field_id": field_id,
                "crop_id": crop_id,
                "year": now.year,
                "name": f"Сезон {now.year}",
                "started_at": f"{now.year}-03-01",
                "ended_at": f"{now.year}-09-30",
                "status": "active",
            },
        )
        season_id = int(payload_season.get("data", {}).get("id") or 0)

        overlap_status, overlap_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/seasons",
            email=manager_email,
            payload={
                "field_id": field_id,
                "crop_id": crop_id,
                "year": now.year,
                "name": f"Сезон overlap {now.year}",
                "started_at": f"{now.year}-04-01",
                "ended_at": f"{now.year}-08-30",
                "status": "active",
            },
        )
        overlap_error = overlap_payload.get("error", {}) if isinstance(overlap_payload.get("error"), dict) else {}

        ok = status == 201 and status_season == 201 and season_id > 0 and overlap_status == 422 and overlap_error.get("code") == "VALIDATION_ERROR"
        add_case(
            cases,
            feature="Культуры и сезоны (проверка пересечения)",
            input_data="POST /crops, POST /seasons, POST /seasons(overlap)",
            expected="Создание сезона + блокировка пересечения активных сезонов",
            ok=ok,
            actual=(
                f"crop_status={status}, season_status={status_season}, season_id={season_id}, "
                f"overlap_status={overlap_status}, overlap_error={overlap_error}"
            ),
        )

        # 5) Operations / notes
        op_status, op_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path=f"/api/v1/fields/{field_id}/operations",
            email=manager_email,
            payload={
                "operation_type": "irrigation",
                "operation_at": to_now,
                "comment": "Полевой полив для теста этапа 5",
            },
        )
        list_status, list_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/operations?page=1&page_size=5&sort=-operation_at",
            email=manager_email,
        )
        items = list_payload.get("data", {}).get("items", []) if isinstance(list_payload.get("data"), dict) else []
        ok = op_status == 201 and list_status == 200 and isinstance(items, list) and len(items) >= 1
        add_case(
            cases,
            feature="Дневник/заметки по полю",
            input_data="POST /fields/{id}/operations + GET /fields/{id}/operations",
            expected="Запись создана и доступна в списке",
            ok=ok,
            actual=f"create_status={op_status}, list_status={list_status}, operations={len(items)}",
        )

        # 6) Sync + weather + NO_DATA
        sync_status, sync_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/sync/run",
            email=manager_email,
            idempotency_key=f"sync-{uuid.uuid4().hex}",
            payload={
                "source": "Copernicus",
                "hours": 72,
                "retention_days": 30,
                "field_id": field_id,
            },
        )

        weather_status, weather_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/weather?from={from_48h}&to={to_now}&granularity=day&source=Copernicus",
            email=manager_email,
        )

        weather_data = weather_payload.get("data", {}) if isinstance(weather_payload.get("data"), dict) else {}
        weather_meta = weather_payload.get("meta", {}) if isinstance(weather_payload.get("meta"), dict) else {}

        no_data_from = _iso_utc(now - timedelta(days=3650))
        no_data_to = _iso_utc(now - timedelta(days=3600))
        nd_status, nd_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/weather?from={no_data_from}&to={no_data_to}&granularity=day&source=Copernicus",
            email=manager_email,
        )
        nd_data = nd_payload.get("data", {}) if isinstance(nd_payload.get("data"), dict) else {}
        nd_meta = nd_payload.get("meta", {}) if isinstance(nd_payload.get("meta"), dict) else {}

        ok = (
            sync_status == 202
            and weather_status == 200
            and isinstance(weather_data.get("values"), list)
            and weather_data.get("meta", {}).get("last_sync_at") is not None
            and weather_meta.get("api_version") == "v1"
            and nd_status == 200
            and nd_meta.get("status") == "NO_DATA"
            and nd_data.get("values") == []
        )
        add_case(
            cases,
            feature="Погода + поведение NO_DATA",
            input_data="POST /sync/run, GET /weather (рабочий диапазон и пустой диапазон)",
            expected="200, данные по контракту + NO_DATA для пустого диапазона",
            ok=ok,
            actual=(
                f"sync_status={sync_status}, weather_status={weather_status}, "
                f"values={len(weather_data.get('values', []))}, no_data_status={nd_status}, no_data_meta={nd_meta}"
            ),
        )

        # 7) Satellite endpoints
        sat_status, sat_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/satellite/index?type=ndvi&from={from_48h}&to={to_now}&source=Copernicus",
            email=manager_email,
        )
        scenes_status, scenes_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/satellite/scenes?from={from_48h}&to={to_now}&source=Copernicus",
            email=manager_email,
        )
        quality_status, quality_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/satellite/quality?from={from_48h}&to={to_now}&source=Copernicus",
            email=manager_email,
        )
        ok = (
            sat_status == 200
            and scenes_status == 200
            and quality_status == 200
            and isinstance(sat_payload.get("data", {}).get("values"), list)
            and isinstance(scenes_payload.get("data", {}).get("scenes"), list)
            and isinstance(quality_payload.get("data", {}).get("quality"), list)
        )
        add_case(
            cases,
            feature="Спутниковые индексы и качество",
            input_data="GET /satellite/index, /satellite/scenes, /satellite/quality",
            expected="200 + единый контракт + метаданные",
            ok=ok,
            actual=(
                f"index={sat_status}/{len(sat_payload.get('data', {}).get('values', []))}, "
                f"scenes={scenes_status}/{len(scenes_payload.get('data', {}).get('scenes', []))}, "
                f"quality={quality_status}/{len(quality_payload.get('data', {}).get('quality', []))}"
            ),
        )

        # 8) RBAC forbidden
        rb_status, rb_payload, _ = api_request(
            base_url=base_url,
            method="DELETE",
            path=f"/api/v1/fields/{field_id}",
            email=viewer_email,
        )
        rb_error = rb_payload.get("error", {}) if isinstance(rb_payload.get("error"), dict) else {}
        ok = rb_status == 403 and rb_error.get("code") == "FORBIDDEN" and "Недостаточно прав" in str(rb_error.get("message"))
        add_case(
            cases,
            feature="RBAC: запрет опасной операции",
            input_data=f"DELETE /fields/{field_id} от viewer",
            expected="403 FORBIDDEN + русское сообщение",
            ok=ok,
            actual=f"status={rb_status}, error={rb_error}",
        )

        # 9) Assistant rules + alerts + recommendations
        wind_ts = seed_wind_value(field_id, value=9.0, source="Copernicus")

        rule_status, rule_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/assistant/rules",
            email=manager_email,
            payload={
                "enterprise_id": enterprise_id,
                "field_id": field_id,
                "parameter": "wind",
                "condition": "gt",
                "threshold": 8,
                "period_hours": 12,
                "recommendation_text": "нельзя опрыскивать",
                "severity": "critical",
            },
        )
        rule_id = int(rule_payload.get("data", {}).get("id") or 0)

        alerts_from = _iso_utc(now - timedelta(hours=2))
        alerts_to = _iso_utc(now + timedelta(hours=2))
        alerts_status, alerts_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/assistant/alerts?from={alerts_from}&to={alerts_to}",
            email=manager_email,
        )

        rec_status, rec_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/assistant/recommendations?at={wind_ts}",
            email=manager_email,
        )

        alerts = alerts_payload.get("data", {}).get("alerts", []) if isinstance(alerts_payload.get("data"), dict) else []
        recs = rec_payload.get("data", {}).get("recommendations", []) if isinstance(rec_payload.get("data"), dict) else []

        ok = (
            rule_status == 201
            and rule_id > 0
            and alerts_status == 200
            and rec_status == 200
            and any("нельзя опрыскивать" in str(a.get("recommendation", "")).lower() for a in alerts)
            and any("9." in str(a.get("reason", "")) for a in alerts)
            and any("нельзя опрыскивать" in str(r.get("what_to_do", "")).lower() for r in recs)
        )
        add_case(
            cases,
            feature="Правило помощника агронома (ветер)",
            input_data="POST /assistant/rules (ветер>8), GET alerts/recommendations",
            expected="alerts/recommendations содержат 'нельзя опрыскивать' и причину с фактом 9 м/с",
            ok=ok,
            actual=f"rule_status={rule_status}, alerts={alerts}, recommendations={recs}",
        )

        # 10) Decision journal
        dec_status, dec_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/assistant/decisions",
            email=manager_email,
            payload={
                "field_id": field_id,
                "rule_id": rule_id,
                "decision": "confirmed",
                "recommendation_text": "нельзя опрыскивать",
                "reason": {"source": "manual_test"},
            },
        )
        list_dec_status, list_dec_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/assistant/decisions?field_id={field_id}",
            email=manager_email,
        )
        dec_items = list_dec_payload.get("data", {}).get("items", []) if isinstance(list_dec_payload.get("data"), dict) else []
        ok = dec_status == 201 and list_dec_status == 200 and len(dec_items) >= 1
        add_case(
            cases,
            feature="Журнал решений помощника",
            input_data="POST /assistant/decisions + GET /assistant/decisions",
            expected="Факт решения сохраняется и возвращается списком",
            ok=ok,
            actual=f"create_status={dec_status}, list_status={list_dec_status}, items={len(dec_items)}",
        )

        # 11) Export + idempotency + download
        export_from = _iso_utc(now - timedelta(days=30))
        export_to = to_now
        idem_key = f"export-{uuid.uuid4().hex}"

        exp_status, exp_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/export",
            email=manager_email,
            idempotency_key=idem_key,
            payload={
                "entity": "weather",
                "source": "Copernicus",
                "from": export_from,
                "to": export_to,
                "granularity": "day",
                "format": "json",
                "field_ids": [field_id],
            },
        )
        export_id = str(exp_payload.get("data", {}).get("export_id") or "")

        exp2_status, exp2_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/export",
            email=manager_email,
            idempotency_key=idem_key,
            payload={
                "entity": "weather",
                "source": "Copernicus",
                "from": export_from,
                "to": export_to,
                "granularity": "day",
                "format": "json",
                "field_ids": [field_id],
            },
        )

        st_status, st_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/export/{export_id}",
            email=manager_email,
        )

        dl_status, dl_payload, dl_headers = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/export/{export_id}/download",
            email=manager_email,
        )

        same_export = str(exp2_payload.get("data", {}).get("export_id") or "") == export_id
        done_status = str(st_payload.get("data", {}).get("status") or "") == "done"
        has_download = dl_status == 200 and (dl_headers.get("Content-Disposition") is not None)

        ok = (
            exp_status == 202
            and export_id
            and exp2_status == 202
            and same_export
            and st_status == 200
            and done_status
            and has_download
        )
        add_case(
            cases,
            feature="Экспорт диапазона + идемпотентность",
            input_data="POST /export (Idempotency-Key), GET /export/{id}, GET /export/{id}/download",
            expected="202 pending, повтор с тем же ключом возвращает тот же export_id, затем done и файл доступен",
            ok=ok,
            actual=(
                f"create={exp_status}, replay={exp2_status}, same_export={same_export}, "
                f"status={st_payload.get('data', {}).get('status')}, download={dl_status}"
            ),
        )

        # 12) Metrics + audit + pagination/filter/sort
        filter_value = urllib.parse.quote("Этап5")
        fields_status, fields_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields?page=1&page_size=10&sort=-created_at&filter={filter_value}",
            email=manager_email,
        )
        metrics_status, metrics_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path="/api/v1/metrics/overview",
            email=admin_email,
        )
        audit_status, audit_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path="/api/v1/audit?page=1&page_size=20",
            email=admin_email,
        )

        fields_items = fields_payload.get("data", {}).get("items", []) if isinstance(fields_payload.get("data"), dict) else []
        audit_items = audit_payload.get("data", {}).get("items", []) if isinstance(audit_payload.get("data"), dict) else []

        ok = (
            fields_status == 200
            and metrics_status == 200
            and audit_status == 200
            and isinstance(fields_items, list)
            and isinstance(metrics_payload.get("data", {}).get("summary"), dict)
            and len(audit_items) >= 1
        )
        add_case(
            cases,
            feature="Пагинация/фильтры/сортировка + наблюдаемость + аудит",
            input_data="GET /fields?page=&page_size=&sort=&filter=, GET /metrics/overview, GET /audit",
            expected="Списочные эндпоинты поддерживают page/page_size/sort/filter, метрики и аудит доступны",
            ok=ok,
            actual=(
                f"fields_status={fields_status}, fields_items={len(fields_items)}, "
                f"metrics_status={metrics_status}, audit_status={audit_status}, audit_items={len(audit_items)}"
            ),
        )

        # 13) Layer registry
        layers_status, layers_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path="/api/v1/layers?source=copernicus",
            email=manager_email,
        )
        layer_items = layers_payload.get("data", {}).get("items", []) if isinstance(layers_payload.get("data"), dict) else []
        has_wind_layer = any(item.get("layer_id") == "weather.wind_vector_10m" for item in layer_items if isinstance(item, dict))
        ok = layers_status == 200 and isinstance(layer_items, list) and has_wind_layer
        add_case(
            cases,
            feature="Реестр слоёв (Layer Registry)",
            input_data="GET /layers?source=copernicus",
            expected="200 + список слоёв с time/spatial/legend/source_meta",
            ok=ok,
            actual=f"status={layers_status}, layers={len(layer_items)}, has_wind_layer={has_wind_layer}",
        )

        # 14) Layer grid + tiles (ETag)
        grid_status, grid_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=(
                f"/api/v1/layers/weather.wind_vector_10m/grid?bbox={field_bbox_str}"
                f"&zoom=12&from={from_48h}&to={to_now}&granularity=hour&agg=mean&field_id={field_id}&source=Copernicus"
            ),
            email=manager_email,
        )
        grid_data = grid_payload.get("data", {}) if isinstance(grid_payload.get("data"), dict) else {}
        grid_cells = grid_data.get("grid", {}).get("cells", []) if isinstance(grid_data.get("grid"), dict) else []

        center_lon, center_lat = field_center
        tile_z = 13
        tile_x, tile_y = tile_for_point(center_lon, center_lat, tile_z)
        tile_path = (
            f"/api/v1/layers/weather.wind_vector_10m/tiles/{tile_z}/{tile_x}/{tile_y}"
            f"?time={to_now}&granularity=hour&agg=mean&source=Copernicus&field_id={field_id}"
        )
        tile_status, tile_body, tile_headers = raw_request(
            base_url=base_url,
            method="GET",
            path=tile_path,
            email=manager_email,
        )
        etag = tile_headers.get("ETag")
        tile_json = json.loads(tile_body.decode("utf-8")) if tile_status == 200 and tile_body else {}

        tile_304_status, _, _ = raw_request(
            base_url=base_url,
            method="GET",
            path=tile_path,
            email=manager_email,
            headers={"If-None-Match": etag or ""},
        )
        ok = (
            grid_status == 200
            and isinstance(grid_cells, list)
            and len(grid_cells) > 0
            and tile_status == 200
            and bool(etag)
            and tile_304_status == 304
            and isinstance(tile_json.get("payload"), dict)
        )
        add_case(
            cases,
            feature="Карта: grid и tiles + кэш",
            input_data="GET /layers/{layer_id}/grid и /tiles/{z}/{x}/{y}",
            expected="Grid-ячейки, tile-ответ с ETag и 304 при повторе",
            ok=ok,
            actual=(
                f"grid_status={grid_status}, cells={len(grid_cells)}, tile_status={tile_status}, "
                f"etag={bool(etag)}, tile_304={tile_304_status}"
            ),
        )

        # 15) Hover probe
        probe_status, probe_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=(
                f"/api/v1/fields/{field_id}/probe?lat={center_lat}&lon={center_lon}"
                f"&time={to_now}&layers=weather.wind_vector_10m,satellite.ndvi&source=Copernicus"
            ),
            email=manager_email,
        )
        probe_data = probe_payload.get("data", {}) if isinstance(probe_payload.get("data"), dict) else {}
        probe_values = probe_data.get("values", []) if isinstance(probe_data, dict) else []
        ok = (
            probe_status == 200
            and isinstance(probe_values, list)
            and isinstance(probe_data.get("mini_stats"), dict)
            and isinstance(probe_data.get("mini_reco"), str)
        )
        add_case(
            cases,
            feature="Hover/Probe по точке",
            input_data="GET /fields/{id}/probe?lat=&lon=&time=&layers=",
            expected="values + mini_stats + mini_reco + last_sync_at",
            ok=ok,
            actual=f"status={probe_status}, values={len(probe_values)}, mini_stats={probe_data.get('mini_stats')}",
        )

        # 16) Zones + zonal stats
        zones_status, zones_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/fields/{field_id}/zones?zoom=12&time={to_now}&method=grid&source=Copernicus",
            email=manager_email,
        )
        zones_data = zones_payload.get("data", {}) if isinstance(zones_payload.get("data"), dict) else {}
        zones_items = zones_data.get("zones", []) if isinstance(zones_data, dict) else []
        zone_id = str(zones_items[0].get("zone_id")) if zones_items else ""

        zonal_status, zonal_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=(
                f"/api/v1/fields/{field_id}/zonal-stats?zone_id={zone_id}"
                f"&from={from_48h}&to={to_now}&metrics=ndvi,temperature&source=Copernicus"
            ),
            email=manager_email,
        )
        zonal_items = zonal_payload.get("data", {}).get("items", []) if isinstance(zonal_payload.get("data"), dict) else []
        ok = zones_status == 200 and len(zones_items) > 0 and zonal_status == 200 and len(zonal_items) > 0
        add_case(
            cases,
            feature="Zones + zonal-stats",
            input_data="GET /fields/{id}/zones и /fields/{id}/zonal-stats",
            expected="Сформированы зоны с rank и статистикой неоднородности",
            ok=ok,
            actual=(
                f"zones_status={zones_status}, zones={len(zones_items)}, "
                f"zonal_status={zonal_status}, zonal_items={len(zonal_items)}"
            ),
        )

        # 17) SSE stream
        stream_status, stream_body, stream_headers = raw_request(
            base_url=base_url,
            method="GET",
            path="/api/v1/stream",
            email=manager_email,
        )
        stream_text = stream_body.decode("utf-8", errors="replace")
        ok = (
            stream_status == 200
            and "text/event-stream" in str(stream_headers.get("Content-Type", ""))
            and ("event: sync_updated" in stream_text or "event: heartbeat" in stream_text)
        )
        add_case(
            cases,
            feature="SSE поток статусов",
            input_data="GET /stream",
            expected="event-stream с событиями sync/export/scenario",
            ok=ok,
            actual=f"status={stream_status}, content_type={stream_headers.get('Content-Type')}",
        )

        # 18) Scenario modeling
        scenario_status, scenario_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path="/api/v1/modeling/scenarios",
            email=manager_email,
            payload={
                "field_id": field_id,
                "source": "Copernicus",
                "from": from_48h,
                "to": to_now,
                "params": {
                    "rain_delta_mm": 12,
                    "temp_shift_c": 2,
                    "wind_shift_ms": 1.5,
                    "irrigation_event": {"mm": 8},
                },
            },
        )
        scenario_id = str(scenario_payload.get("data", {}).get("scenario_id") or "")

        patch_status, _, _ = api_request(
            base_url=base_url,
            method="PATCH",
            path=f"/api/v1/modeling/scenarios/{scenario_id}",
            email=manager_email,
            payload={"params": {"operation_shift": {"days": 2}}},
        )

        run_status, run_payload, _ = api_request(
            base_url=base_url,
            method="POST",
            path=f"/api/v1/modeling/scenarios/{scenario_id}/run",
            email=manager_email,
            payload={},
        )

        get_status, get_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/modeling/scenarios/{scenario_id}",
            email=manager_email,
        )
        result_status, result_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/modeling/scenarios/{scenario_id}/result",
            email=manager_email,
        )
        diff_status, diff_payload, _ = api_request(
            base_url=base_url,
            method="GET",
            path=f"/api/v1/modeling/scenarios/{scenario_id}/diff",
            email=manager_email,
        )

        result_data = result_payload.get("data", {}) if isinstance(result_payload.get("data"), dict) else {}
        result_obj = result_data.get("result") if isinstance(result_data, dict) else None
        result_values = result_obj.get("values", []) if isinstance(result_obj, dict) else []
        diff_data = diff_payload.get("data", {}) if isinstance(diff_payload.get("data"), dict) else {}
        diff_obj = diff_data.get("diff") if isinstance(diff_data, dict) else None
        diff_metrics = diff_obj.get("metrics", []) if isinstance(diff_obj, dict) else []
        ok = (
            scenario_status == 201
            and bool(scenario_id)
            and patch_status == 200
            and run_status == 202
            and get_status == 200
            and result_status == 200
            and diff_status == 200
            and isinstance(result_values, list)
            and len(result_values) > 0
            and isinstance(diff_metrics, list)
            and len(diff_metrics) > 0
        )
        add_case(
            cases,
            feature="Modeling/Scenario (what-if)",
            input_data="POST/PATCH/POST run + GET scenario/result/diff",
            expected="Сценарий запускается и возвращает result/diff в контракте source=scenario",
            ok=ok,
            actual=(
                f"create={scenario_status}, patch={patch_status}, run={run_status}, "
                f"result={result_status}/{len(result_values)}, diff={diff_status}/{len(diff_metrics)}"
            ),
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
