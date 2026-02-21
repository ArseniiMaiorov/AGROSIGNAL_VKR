from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
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


def run_command(args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        input=input_text,
        capture_output=True,
        check=False,
    )


def run_checked(args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    result = run_command(args, input_text=input_text)
    if result.returncode != 0:
        if result.stdout.strip():
            print(result.stdout.strip(), file=sys.stderr)
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"Команда завершилась с ошибкой: {' '.join(args)}")
    return result


def ensure_db_ready() -> None:
    print("Поднимаю БД для миграций...")
    run_checked(COMPOSE_CMD + ["up", "-d", "--wait", "db"])


def psql_exec(sql: str) -> None:
    run_checked(DB_EXEC_PREFIX + ["-f", "-"], input_text=sql)


def psql_query(sql: str) -> list[str]:
    result = run_checked(DB_EXEC_PREFIX + ["-At", "-c", sql])
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def bootstrap_migrations_table() -> None:
    psql_exec(
        """
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def migration_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_applied_migrations() -> dict[str, str]:
    rows = psql_query("SELECT version || '|' || checksum FROM schema_migrations ORDER BY version;")
    applied: dict[str, str] = {}
    for row in rows:
        version, checksum = row.split("|", 1)
        applied[version] = checksum
    return applied


def apply_migrations() -> None:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        raise RuntimeError("Не найдены файлы миграций в backend/migrations")

    applied = load_applied_migrations()

    for migration_file in migration_files:
        version = migration_file.name
        checksum = migration_checksum(migration_file)

        if version in applied:
            if applied[version] != checksum:
                raise RuntimeError(
                    f"Контрольная сумма миграции {version} изменилась. "
                    "Изменять уже применённые миграции запрещено."
                )
            print(f"[SKIP] {version} уже применена")
            continue

        print(f"[APPLY] {version}")
        psql_exec(migration_file.read_text(encoding="utf-8"))
        psql_exec(
            "INSERT INTO schema_migrations (version, checksum) VALUES "
            f"('{version}', '{checksum}');"
        )


def main() -> int:
    try:
        ensure_db_ready()
        bootstrap_migrations_table()
        apply_migrations()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print("Миграции успешно применены.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
