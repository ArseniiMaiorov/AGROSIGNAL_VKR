from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from stage3_cli import DbClient, Stage3Error
from stage4_proxy import (
    Stage4Error,
    ensure_stage4_admin,
    get_degradation_status,
    get_proxy_metrics,
    get_proxy_settings,
    get_request_log,
    perform_provider_request,
    run_proxy_health_check,
    set_proxy_settings,
)


def _print_json(payload: dict[str, Any] | list[Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Некорректное булево значение: {value}")


def _parse_backoff(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    try:
        values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise Stage4Error("Параметр --backoff должен содержать целые числа через запятую") from exc

    if not values:
        raise Stage4Error("Параметр --backoff не может быть пустым")
    if any(item <= 0 for item in values):
        raise Stage4Error("Параметр --backoff должен содержать только значения > 0")
    return values


def _resolve_proxy_update(args: argparse.Namespace, current: dict[str, Any]) -> dict[str, Any]:
    backoff = _parse_backoff(args.backoff)

    if args.clear_bypass and args.bypass_host:
        raise Stage4Error("Нельзя одновременно использовать --clear-bypass и --bypass-host")

    if args.clear_bypass:
        bypass_hosts: list[str] = []
    elif args.bypass_host is not None:
        bypass_hosts = [host.strip() for host in args.bypass_host if host.strip()]
    else:
        bypass_hosts = [str(item) for item in current.get("bypass_hosts", [])]

    return {
        "enabled": current["proxy_enabled"] if args.enabled is None else args.enabled,
        "mode": current["proxy_mode"] if args.mode is None else args.mode,
        "proxy_endpoint": current.get("proxy_endpoint") if args.proxy_endpoint is None else args.proxy_endpoint,
        "copernicus_via_proxy": (
            current["copernicus_via_proxy"] if args.copernicus_via_proxy is None else args.copernicus_via_proxy
        ),
        "nasa_via_proxy": current["nasa_via_proxy"] if args.nasa_via_proxy is None else args.nasa_via_proxy,
        "bypass_hosts": bypass_hosts,
        "bypass_policy": current["bypass_policy"] if args.bypass_policy is None else args.bypass_policy,
        "timeout_seconds": current["timeout_seconds"] if args.timeout_seconds is None else args.timeout_seconds,
        "max_retries": current["max_retries"] if args.max_retries is None else args.max_retries,
        "backoff_schedule": current["backoff_schedule"] if backoff is None else backoff,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 4: proxy-контур для загрузчиков датасетов")
    sub = parser.add_subparsers(dest="command", required=True)

    ensure_admin = sub.add_parser("ensure-admin")
    ensure_admin.add_argument("--email", required=True)

    proxy_get = sub.add_parser("proxy-get")
    proxy_get.add_argument("--admin-email", required=True)

    proxy_set = sub.add_parser("proxy-set")
    proxy_set.add_argument("--admin-email", required=True)
    proxy_set.add_argument("--enabled", type=_parse_bool)
    proxy_set.add_argument("--mode", choices=["global", "per_provider"])
    proxy_set.add_argument("--proxy-endpoint")
    proxy_set.add_argument("--copernicus-via-proxy", type=_parse_bool)
    proxy_set.add_argument("--nasa-via-proxy", type=_parse_bool)
    proxy_set.add_argument("--bypass-host", action="append")
    proxy_set.add_argument("--clear-bypass", action="store_true")
    proxy_set.add_argument("--bypass-policy", choices=["direct", "force_proxy"])
    proxy_set.add_argument("--timeout-seconds", type=int)
    proxy_set.add_argument("--max-retries", type=int)
    proxy_set.add_argument("--backoff")

    request = sub.add_parser("request")
    request.add_argument("--provider", required=True)
    request.add_argument("--module", required=True)
    request.add_argument("--url", required=True)
    request.add_argument("--request-id")
    request.add_argument("--no-sync-state", action="store_true")

    health = sub.add_parser("health-check")
    health.add_argument("--admin-email", required=True)
    health.add_argument("--provider", required=True)
    health.add_argument("--module", required=True)
    health.add_argument("--source-url", required=True)

    degradation = sub.add_parser("degradation-status")
    degradation.add_argument("--provider", required=True)

    metrics = sub.add_parser("metrics")
    metrics.add_argument("--admin-email", required=True)

    request_log = sub.add_parser("request-log")
    request_log.add_argument("--request-id", required=True)
    request_log.add_argument("--admin-email", required=True)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    db = DbClient()

    try:
        db.ensure_ready()

        if args.command == "ensure-admin":
            user_id = ensure_stage4_admin(db, args.email)
            _print_json({"admin_email": args.email, "user_id": user_id})
            return 0

        if args.command == "proxy-get":
            _print_json(get_proxy_settings(db, admin_email=args.admin_email))
            return 0

        if args.command == "proxy-set":
            current = get_proxy_settings(db, admin_email=args.admin_email)
            merged = _resolve_proxy_update(args, current)
            _print_json(
                set_proxy_settings(
                    db,
                    admin_email=args.admin_email,
                    enabled=bool(merged["enabled"]),
                    mode=str(merged["mode"]),
                    proxy_endpoint=str(merged["proxy_endpoint"] or ""),
                    copernicus_via_proxy=bool(merged["copernicus_via_proxy"]),
                    nasa_via_proxy=bool(merged["nasa_via_proxy"]),
                    bypass_hosts=[str(item) for item in merged["bypass_hosts"]],
                    bypass_policy=str(merged["bypass_policy"]),
                    timeout_seconds=int(merged["timeout_seconds"]),
                    max_retries=int(merged["max_retries"]),
                    backoff_schedule=[int(item) for item in merged["backoff_schedule"]],
                )
            )
            return 0

        if args.command == "request":
            _print_json(
                perform_provider_request(
                    db,
                    provider=args.provider,
                    module_name=args.module,
                    url=args.url,
                    request_id=args.request_id,
                    update_sync_state=not bool(args.no_sync_state),
                )
            )
            return 0

        if args.command == "health-check":
            _print_json(
                run_proxy_health_check(
                    db,
                    admin_email=args.admin_email,
                    provider=args.provider,
                    module_name=args.module,
                    source_url=args.source_url,
                )
            )
            return 0

        if args.command == "degradation-status":
            _print_json(get_degradation_status(db, provider=args.provider))
            return 0

        if args.command == "metrics":
            _print_json(get_proxy_metrics(db, admin_email=args.admin_email))
            return 0

        if args.command == "request-log":
            _print_json(get_request_log(db, request_id=args.request_id, admin_email=args.admin_email))
            return 0

        raise Stage4Error("Неизвестная команда")
    except (Stage3Error, Stage4Error) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
