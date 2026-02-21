from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _parse_env_pairs(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Ожидается формат KEY=VALUE, получено: {raw}")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError("Пустое имя переменной окружения")
        result[key] = value
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Запуск процесса в detached-режиме")
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--cwd", default=".")
    parser.add_argument("--env", action="append", default=[])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if not args.command:
        print("Не указана команда для запуска", file=sys.stderr)
        return 1

    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print("Не указана команда после '--'", file=sys.stderr)
        return 1

    try:
        extra_env = _parse_env_pairs(list(args.env))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    env = os.environ.copy()
    env.update(extra_env)

    cwd = Path(args.cwd).resolve()
    log_path = Path(args.log_file).resolve()
    pid_path = Path(args.pid_file).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )

    pid_path.write_text(str(process.pid), encoding="utf-8")
    print(process.pid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
