from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = ROOT / "tests" / "fixtures" / "geo"
REPORT_PATH = ROOT / "reports" / "tests" / f"{date.today().isoformat()}_stage2_geometry.md"
COMPOSE_CMD = ["docker", "compose", "-f", "docker-compose.yml"]
DB_EXEC_PREFIX = COMPOSE_CMD + [
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


@dataclass
class CaseResult:
    feature: str
    fixture: str
    expected: str
    actual: str
    status: str


@dataclass
class SqlResult:
    success: bool
    stdout: str
    stderr: str


def run_command(args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        input=input_text,
        capture_output=True,
        check=False,
    )


def psql_exec(sql: str) -> SqlResult:
    result = run_command(DB_EXEC_PREFIX + ["-At", "-f", "-"], input_text=sql)
    return SqlResult(
        success=result.returncode == 0,
        stdout=result.stdout.strip(),
        stderr=result.stderr.strip(),
    )


def ensure_db_ready() -> None:
    run_command(COMPOSE_CMD + ["up", "-d", "--wait", "db"])


def reset_data() -> None:
    reset_sql = """
    TRUNCATE TABLE work_journal, fields, seasons, app_users, roles, crops, enterprises
    RESTART IDENTITY CASCADE;

    INSERT INTO enterprises (name) VALUES ('ООО Тест-Агро');
    INSERT INTO roles (code, name) VALUES ('agronom', 'Агроном');
    INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash)
    VALUES (1, 1, 'agronom@test.local', 'Тестовый агроном', 'hash');
    INSERT INTO crops (name) VALUES ('Пшеница');
    INSERT INTO seasons (enterprise_id, crop_id, year, name, started_at, ended_at)
    VALUES (1, 1, 2026, 'Сезон 2026', '2026-03-01', '2026-09-30');
    """
    result = psql_exec(reset_sql)
    if not result.success:
        raise RuntimeError(f"Не удалось подготовить данные: {result.stderr}")


def load_fixture(name: str) -> tuple[str, int]:
    fixture_path = FIXTURES_DIR / name
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    geometry = payload["geometry"]
    srid = int(payload.get("properties", {}).get("srid", 4326))
    return json.dumps(geometry, ensure_ascii=False), srid


def insert_field(field_name: str, fixture_name: str) -> SqlResult:
    geometry_json, srid = load_fixture(fixture_name)
    sql = f"""
    INSERT INTO fields (enterprise_id, season_id, name, geom)
    VALUES (
        1,
        1,
        '{field_name}',
        ST_SetSRID(ST_GeomFromGeoJSON($${geometry_json}$$), {srid})
    )
    RETURNING id, area_ha;
    """
    return psql_exec(sql)


def run_cases() -> list[CaseResult]:
    results: list[CaseResult] = []

    ok_result = insert_field("Поле корректное", "field_ok.geojson")
    if ok_result.success and ok_result.stdout:
        status = "PASS"
        actual = f"Успешная вставка: {ok_result.stdout}"
    else:
        status = "FAIL"
        actual = f"Ошибка: {ok_result.stderr or ok_result.stdout}"
    results.append(
        CaseResult(
            feature="Добавление валидного полигона",
            fixture="fixtures/geo/field_ok.geojson",
            expected="INSERT успешен, id != null, area_ha > 0",
            actual=actual,
            status=status,
        )
    )

    self_intersect_result = insert_field("Поле самопересечение", "field_self_intersect.geojson")
    if (not self_intersect_result.success) and (
        "Полигон самопересекается" in self_intersect_result.stderr
    ):
        status = "PASS"
        actual = "Получена ожидаемая ошибка: Полигон самопересекается"
    else:
        status = "FAIL"
        actual = (
            f"Неожиданный результат. stdout: {self_intersect_result.stdout}; "
            f"stderr: {self_intersect_result.stderr}"
        )
    results.append(
        CaseResult(
            feature="Отклонение самопересекающегося полигона",
            fixture="fixtures/geo/field_self_intersect.geojson",
            expected="Ошибка: Полигон самопересекается",
            actual=actual,
            status=status,
        )
    )

    wrong_srid_result = insert_field("Поле неверный SRID", "field_wrong_srid.geojson")
    if (not wrong_srid_result.success) and (
        "Неверная система координат" in wrong_srid_result.stderr
    ):
        status = "PASS"
        actual = "Получена ожидаемая ошибка: Неверная система координат"
    else:
        status = "FAIL"
        actual = (
            f"Неожиданный результат. stdout: {wrong_srid_result.stdout}; "
            f"stderr: {wrong_srid_result.stderr}"
        )
    results.append(
        CaseResult(
            feature="Отклонение полигона с неверным SRID",
            fixture="fixtures/geo/field_wrong_srid.geojson",
            expected="Ошибка: Неверная система координат: ожидается EPSG:4326",
            actual=actual,
            status=status,
        )
    )

    return results


def write_report(results: list[CaseResult]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "# Протокол тестирования этапа 2: модель данных и валидация геометрии",
        "",
    ]

    for case in results:
        lines.extend(
            [
                f"Фича: {case.feature}",
                "Вход:",
                f"  - {case.fixture}",
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
    ensure_db_ready()

    try:
        reset_data()
    except RuntimeError as exc:
        print(str(exc))
        return 1

    results = run_cases()
    write_report(results)

    failed = [case for case in results if case.status == "FAIL"]
    print(f"Сформирован отчёт: {REPORT_PATH.relative_to(ROOT)}")

    if failed:
        for case in failed:
            print(f"FAIL: {case.feature} -> {case.actual}")
        return 1

    for case in results:
        print(f"PASS: {case.feature}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
