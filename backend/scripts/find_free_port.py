from __future__ import annotations

import argparse
import socket
import sys


def is_port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def choose_port(start: int, host: str, max_attempts: int) -> int:
    if start < 1 or start > 65535:
        raise ValueError("Стартовый порт должен быть в диапазоне 1..65535")

    for port in range(start, min(start + max_attempts, 65536)):
        if is_port_free(host, port):
            return port

    raise RuntimeError("Не удалось найти свободный порт в заданном диапазоне")


def main() -> int:
    parser = argparse.ArgumentParser(description="Поиск свободного TCP-порта")
    parser.add_argument("--start", type=int, default=18000, help="Стартовый порт")
    parser.add_argument("--host", default="127.0.0.1", help="Хост для проверки")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=200,
        help="Максимум проверяемых портов от стартового",
    )
    args = parser.parse_args()

    try:
        port = choose_port(args.start, args.host, args.max_attempts)
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
