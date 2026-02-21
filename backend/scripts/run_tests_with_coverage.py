from __future__ import annotations

import sys
import threading
import trace
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
TESTS_DIR = ROOT / "tests"


def _collect_source_files() -> list[Path]:
    return sorted(path for path in SRC_DIR.rglob("*.py") if path.is_file())


def _executable_lines(path: Path) -> set[int]:
    lines = trace._find_executable_linenos(str(path))  # type: ignore[attr-defined]
    return {int(line) for line in lines if isinstance(line, int) and line > 0}


def _executed_lines(counts: dict[tuple[str, int], int], path: Path) -> set[int]:
    resolved = path.resolve()
    return {
        lineno
        for (filename, lineno), count in counts.items()
        if count > 0 and Path(filename).resolve() == resolved and lineno > 0
    }


def _discover_and_run_tests() -> unittest.result.TestResult:
    suite = unittest.defaultTestLoader.discover(str(TESTS_DIR), pattern="test_*.py")
    return unittest.TextTestRunner(verbosity=2).run(suite)


def run() -> int:
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))

    tracer = trace.Trace(count=True, trace=False)

    threading.settrace(tracer.globaltrace)
    try:
        result = tracer.runfunc(_discover_and_run_tests)
    finally:
        threading.settrace(None)

    if not result.wasSuccessful():
        return 1

    counts = tracer.results().counts
    source_files = _collect_source_files()

    total_executable = 0
    total_covered = 0
    uncovered_report: list[str] = []

    for source_file in source_files:
        executable = _executable_lines(source_file)
        if not executable:
            continue

        executed = _executed_lines(counts, source_file)
        missing = sorted(executable - executed)

        total_executable += len(executable)
        total_covered += len(executable) - len(missing)

        if missing:
            relative_path = source_file.relative_to(ROOT)
            uncovered_report.append(f"{relative_path}: {', '.join(map(str, missing))}")

    coverage = 100.0 if total_executable == 0 else (total_covered / total_executable) * 100.0

    print(f"\nИтоговое покрытие по backend/src: {coverage:.2f}%")

    if uncovered_report:
        print("Непокрытые строки:")
        for report_line in uncovered_report:
            print(f"- {report_line}")
        return 2

    if coverage < 100.0:
        print("Покрытие ниже 100%.")
        return 3

    print("Покрытие 100% достигнуто.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
