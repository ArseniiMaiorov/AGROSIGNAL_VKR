from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "reports" / "tests" / f"{date.today().isoformat()}_stage3_workflows.md"


@dataclass
class CaseResult:
    feature: str
    input_data: str
    expected: str
    actual: str
    status: str


def run_cli(*args: str) -> dict[str, Any]:
    cmd = [sys.executable, "scripts/stage3_cli.py", *args]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Команда {' '.join(args)} завершилась ошибкой: {stderr}")

    stdout = result.stdout.strip()
    if not stdout:
        return {}
    return json.loads(stdout)


def assert_desc(items: list[dict[str, Any]], key: str) -> bool:
    values = [str(item.get(key, "")) for item in items]
    return values == sorted(values, reverse=True)


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
    lines: list[str] = ["# Протокол тестирования этапа 3: контракт, провайдеры, drill-down, экспорт, TTL", ""]

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

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    cases: list[CaseResult] = []

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    range_start = now - timedelta(days=60)
    range_end = now

    field_id: int | None = None

    # 1) Синхронизация
    try:
        sync_sources = ["Copernicus", "NASA", "Mock"]
        sync_ok = True
        sync_messages: list[str] = []
        for source in sync_sources:
            sync_report = run_cli("sync", "--source", source, "--hours", "1500", "--retention-days", "30")
            field_id = int(sync_report["field_id"]) if field_id is None else field_id

            status = run_cli("sync-status", "--source", source)
            ok = status.get("status") == "ok" and bool(status.get("last_success_at"))
            sync_ok = sync_ok and ok
            sync_messages.append(f"{source}: status={status.get('status')}, last_success_at={status.get('last_success_at')}")

        add_case(
            cases,
            feature="Синхронизация источников",
            input_data="run sync для Copernicus/NASA/Mock, 1500 часов",
            expected="Обновлены last_sync_at/last_success_at и данные доступны",
            ok=sync_ok,
            actual="; ".join(sync_messages),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Синхронизация источников",
            input_data="run sync для Copernicus/NASA/Mock, 1500 часов",
            expected="Обновлены last_sync_at/last_success_at и данные доступны",
            ok=False,
            actual=str(exc),
        )
        write_report(cases)
        print(f"Сформирован отчёт: {REPORT_PATH.relative_to(ROOT)}")
        return 1

    assert field_id is not None

    # 2) Drill-down
    month_query: dict[str, Any] = {}
    day_query: dict[str, Any] = {}
    hour_query: dict[str, Any] = {}
    point_query: dict[str, Any] = {}

    try:
        month_query = run_cli(
            "query",
            "--source",
            "Copernicus",
            "--field-id",
            str(field_id),
            "--from",
            range_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--to",
            range_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--granularity",
            "month",
        )

        month_bins = month_query.get("time_bins", [])
        ok_month = bool(month_bins) and assert_desc(month_bins, "bucket_start")

        first_month = datetime.fromisoformat(month_bins[0]["bucket_start"].replace("Z", "+00:00"))
        month_start = first_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)

        day_query = run_cli(
            "query",
            "--source",
            "Copernicus",
            "--field-id",
            str(field_id),
            "--from",
            month_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--to",
            month_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--granularity",
            "day",
        )
        day_bins = day_query.get("time_bins", [])
        ok_day = bool(day_bins) and assert_desc(day_bins, "bucket_start")

        first_day = datetime.fromisoformat(day_bins[0]["bucket_start"].replace("Z", "+00:00"))
        day_start = first_day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)

        hour_query = run_cli(
            "query",
            "--source",
            "Copernicus",
            "--field-id",
            str(field_id),
            "--from",
            day_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--to",
            day_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--granularity",
            "hour",
        )
        hour_bins = hour_query.get("time_bins", [])
        ok_hour = bool(hour_bins) and assert_desc(hour_bins, "bucket_start")

        first_hour = datetime.fromisoformat(hour_bins[0]["bucket_start"].replace("Z", "+00:00"))
        hour_start = first_hour.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1) - timedelta(seconds=1)

        point_query = run_cli(
            "query",
            "--source",
            "Copernicus",
            "--field-id",
            str(field_id),
            "--from",
            hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--to",
            hour_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--granularity",
            "point",
        )
        point_records = point_query.get("records", [])
        ok_point = bool(point_records)

        add_case(
            cases,
            feature="Drill-down по временной шкале",
            input_data="2 месяца -> месяц -> день -> часы -> час",
            expected="Сводка + bins desc на каждом уровне и точечная статистика на часе",
            ok=ok_month and ok_day and ok_hour and ok_point,
            actual=(
                f"month_bins={len(month_bins)}, day_bins={len(day_bins)}, "
                f"hour_bins={len(hour_bins)}, point_records={len(point_records)}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Drill-down по временной шкале",
            input_data="2 месяца -> месяц -> день -> часы -> час",
            expected="Сводка + bins desc на каждом уровне и точечная статистика на часе",
            ok=False,
            actual=str(exc),
        )

    # 3) Экспорт
    dataset_id = ""
    try:
        export_create = run_cli(
            "export-create",
            "--source",
            "Copernicus",
            "--field-id",
            str(field_id),
            "--from",
            range_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--to",
            range_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--granularity",
            "day",
            "--format",
            "json",
        )
        dataset_id = str(export_create["dataset_id"])

        processed = run_cli("export-process", "--dataset-id", dataset_id)
        status = run_cli("export-status", "--dataset-id", dataset_id)

        file_path = status.get("export_file_path")
        exists = bool(file_path) and (ROOT / str(file_path)).exists()
        ready = status.get("export_status") == "ready"

        add_case(
            cases,
            feature="Экспорт данных по диапазону",
            input_data="export-create + export-process для диапазона 60 дней",
            expected="Создана задача, сформирован файл, статус ready",
            ok=ready and exists and int(processed.get("count", 0)) >= 1,
            actual=f"dataset_id={dataset_id}, status={status.get('export_status')}, file={file_path}",
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Экспорт данных по диапазону",
            input_data="export-create + export-process для диапазона 60 дней",
            expected="Создана задача, сформирован файл, статус ready",
            ok=False,
            actual=str(exc),
        )

    # 4) TTL и предупреждение
    try:
        if not dataset_id:
            raise RuntimeError("dataset_id не получен на шаге экспорта")

        run_cli("dataset-set-expiry", "--dataset-id", dataset_id, "--hours", "23")
        ttl = run_cli("ttl-check")
        warned = dataset_id in ttl.get("dataset_ids", [])

        extended = run_cli("dataset-extend", "--dataset-id", dataset_id, "--days", "10")
        extended_ok = int(extended.get("extended_count", 0)) >= 1

        view = run_cli("dataset-view", "--dataset-id", dataset_id, "--granularity", "day")
        view_ok = bool(view.get("time_bins")) and bool(view.get("summary"))

        add_case(
            cases,
            feature="TTL и предупреждение",
            input_data="dataset expiry=23h -> ttl-check -> dataset-extend + dataset-view",
            expected="Предупреждение сформировано, срок продлён, просмотр доступен",
            ok=warned and extended_ok and view_ok,
            actual=(
                f"warned={warned}, extended_count={extended.get('extended_count')}, "
                f"view_bins={len(view.get('time_bins', []))}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="TTL и предупреждение",
            input_data="dataset expiry=23h -> ttl-check -> dataset-extend + dataset-view",
            expected="Предупреждение сформировано, срок продлён, просмотр доступен",
            ok=False,
            actual=str(exc),
        )

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
