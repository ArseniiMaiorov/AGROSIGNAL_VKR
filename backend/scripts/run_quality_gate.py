from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_step(args: list[str], title: str) -> None:
    print(f"[STEP] {title}", flush=True)
    result = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout.strip():
        print(result.stdout.strip(), flush=True)
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr, flush=True)
    if result.returncode != 0:
        raise RuntimeError(f"Шаг '{title}' завершился с ошибкой")


def main() -> int:
    steps = [
        ([sys.executable, "scripts/run_tests_with_coverage.py"], "Unit-тесты + покрытие backend/src"),
        ([sys.executable, "scripts/migrate.py"], "Миграции"),
        ([sys.executable, "scripts/test_stage2_geometry.py"], "Интеграционные тесты этапа 2"),
        ([sys.executable, "scripts/test_stage3_workflows.py"], "Интеграционные тесты этапа 3"),
        ([sys.executable, "scripts/test_stage4_proxy.py"], "Интеграционные тесты этапа 4"),
        ([sys.executable, "scripts/test_stage5_api.py"], "Интеграционные/API тесты этапа 5"),
        ([sys.executable, "scripts/test_stage6_algorithms.py"], "Интеграционные/алгоритмические тесты этапа 6"),
    ]

    try:
        for args, title in steps:
            run_step(args, title)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print("[OK] Полный quality gate пройден", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
