from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from stage3_cli import DbClient, Stage3Error, run_cycle


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_int_env(name: str, default: int, min_value: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise Stage3Error(f"Переменная {name} должна быть целым числом") from exc

    if value < min_value:
        raise Stage3Error(f"Переменная {name} должна быть >= {min_value}")
    return value


def main() -> int:
    interval_seconds = _read_int_env("STAGE_SCHEDULER_INTERVAL_SECONDS", 900, 10)
    sync_hours = _read_int_env("STAGE_SCHEDULER_SYNC_HOURS", 24, 1)
    retention_days = _read_int_env("STAGE_SCHEDULER_RETENTION_DAYS", 30, 1)

    db = DbClient()
    db.ensure_ready()

    print(
        json.dumps(
            {
                "event": "scheduler_started",
                "timestamp": _now_utc(),
                "interval_seconds": interval_seconds,
                "sync_hours": sync_hours,
                "retention_days": retention_days,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    while True:
        started_at = time.perf_counter()
        try:
            report = run_cycle(db, hours=sync_hours, retention_days=retention_days)
            event = {
                "event": "scheduler_cycle",
                "timestamp": _now_utc(),
                "status": "ok",
                "sync_sources": [item.get("source") for item in report.get("sync", [])],
                "exports_count": int(report.get("exports", {}).get("count", 0)),
                "ttl_warned_count": int(report.get("ttl", {}).get("warned_count", 0)),
            }
        except Exception as exc:  # noqa: BLE001
            event = {
                "event": "scheduler_cycle",
                "timestamp": _now_utc(),
                "status": "error",
                "error": str(exc),
            }

        print(json.dumps(event, ensure_ascii=False), flush=True)

        elapsed = time.perf_counter() - started_at
        sleep_for = max(1.0, float(interval_seconds) - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
