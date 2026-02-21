from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any


def check_url(url: str, timeout: float) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "http_status": int(getattr(response, "status", 200)),
                "body_preview": body[:500],
                "error": None,
            }
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "http_status": int(exc.code),
            "body_preview": payload[:500],
            "error": f"HTTP {exc.code}",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "http_status": None,
            "body_preview": "",
            "error": str(exc),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Проверка HTTP-доступности endpoint")
    parser.add_argument("--url", required=True)
    parser.add_argument("--timeout", type=float, default=3.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = check_url(args.url, args.timeout)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if result["ok"]:
            print(f"OK: {args.url} -> {result['http_status']}")
        else:
            print(f"FAIL: {args.url} -> {result['error']}", file=sys.stderr)

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
