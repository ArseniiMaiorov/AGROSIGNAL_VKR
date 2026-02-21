from __future__ import annotations

import json
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
        with urllib.request.urlopen(request, timeout=8) as response:
            text = response.read().decode("utf-8")
            data = json.loads(text) if text else {}
            return int(response.status), data, dict(response.headers)
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8")
        data = json.loads(text) if text else {}
        return int(exc.code), data, dict(exc.headers)


def load_geometry(name: str) -> dict[str, Any]:
    payload = json.loads((FIXTURES_GEO / name).read_text(encoding="utf-8"))
    return payload["geometry"]


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
