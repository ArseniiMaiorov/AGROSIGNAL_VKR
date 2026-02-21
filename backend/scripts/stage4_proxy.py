from __future__ import annotations

import json
import os
import random
import re
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from stage3_cli import DbClient

VALID_SOURCES = {"Copernicus", "NASA", "Mock"}
ALLOWED_PROXY_MODULE_PREFIXES = (
    "providers/copernicus/",
    "providers/nasa/",
    "datasets/download/",
)
DEFAULT_BACKOFF = [1, 5, 15]
RETRY_HTTP_CODES = {429, 502, 503, 504}


class Stage4Error(RuntimeError):
    pass


@dataclass
class ProxyDecision:
    use_proxy: bool
    reason: str


@dataclass
class RequestOutcome:
    success: bool
    provider: str
    module_name: str
    request_id: str
    target_host: str
    proxy_used: bool
    retry_count: int
    duration_ms: int
    bytes_downloaded: int
    http_status: int | None
    error_class: str | None
    error_message: str | None


@dataclass
class ProxyCheckOutcome:
    status: str
    latency_ms: int | None
    error_class: str | None
    reason: str


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_json_field(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return default


def _is_fast_mode() -> bool:
    return os.environ.get("STAGE4_FAST_MODE", "0") == "1"


def _validate_source(provider: str) -> None:
    if provider not in VALID_SOURCES:
        raise Stage4Error(f"Неподдерживаемый провайдер: {provider}")


def _module_in_proxy_scope(module_name: str) -> bool:
    return any(module_name.startswith(prefix) for prefix in ALLOWED_PROXY_MODULE_PREFIXES)


def _normalize_host(host: str) -> str:
    return host.strip().lower()


def _host_matches(host: str, pattern: str) -> bool:
    host_n = _normalize_host(host)
    pattern_n = _normalize_host(pattern)

    if not pattern_n:
        return False
    if pattern_n.startswith("*."):
        suffix = pattern_n[1:]
        return host_n.endswith(suffix)
    return host_n == pattern_n


def _sanitize_proxy_endpoint(proxy_endpoint: str | None) -> str | None:
    if not proxy_endpoint:
        return None

    parsed = urllib.parse.urlsplit(proxy_endpoint)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{host}{port}"

    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _sanitize_text(value: str) -> str:
    if not value:
        return value

    sanitized = value
    sanitized = re.sub(r"(?i)(https?://)([^/@:\s]+):([^/@\s]+)@", r"\1***:***@", sanitized)
    sanitized = re.sub(r"(?i)(password|passwd|token|secret)=([^&\s]+)", r"\1=***", sanitized)
    return sanitized


def _proxy_endpoint_with_credentials(proxy_endpoint: str | None) -> str | None:
    if not proxy_endpoint:
        return None

    parsed = urllib.parse.urlsplit(proxy_endpoint)
    host = parsed.hostname or ""
    if not host:
        return None

    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")

    auth_prefix = ""
    if username and password:
        user = urllib.parse.quote(username, safe="")
        pwd = urllib.parse.quote(password, safe="")
        auth_prefix = f"{user}:{pwd}@"

    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{auth_prefix}{host}{port}"

    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _classify_exception(exc: Exception, http_status: int | None = None) -> str:
    if http_status in {401, 403}:
        return "auth"

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in {401, 403}:
            return "auth"
        return "http"

    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, socket.timeout):
            return "timeout"
        if isinstance(reason, socket.gaierror):
            return "dns"
        if isinstance(reason, ssl.SSLError):
            return "tls"

        text = str(reason)
        if "CERTIFICATE_VERIFY_FAILED" in text or "TLS" in text or "SSL" in text:
            return "tls"
        if "Name or service not known" in text or "Temporary failure in name resolution" in text:
            return "dns"
        if "timed out" in text.lower():
            return "timeout"

        return "network"

    if isinstance(exc, ssl.SSLError):
        return "tls"

    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "timeout"

    if isinstance(exc, socket.gaierror):
        return "dns"

    return "unknown"


def _is_retryable(error_class: str, http_status: int | None, retry_count: int, max_retries: int) -> bool:
    if retry_count >= max_retries:
        return False

    if error_class == "timeout":
        return True

    if http_status in RETRY_HTTP_CODES:
        return True

    return False


def _backoff_sleep(backoff_schedule: list[int], retry_count: int) -> None:
    if _is_fast_mode():
        return

    base = backoff_schedule[min(retry_count, len(backoff_schedule) - 1)]
    jitter = random.uniform(0.0, 0.4)
    time.sleep(base + jitter)


def _ensure_proxy_settings_row(db: DbClient) -> None:
    db.exec_checked(
        """
        INSERT INTO proxy_settings (id)
        VALUES (1)
        ON CONFLICT (id) DO NOTHING;
        """
    )


def _load_proxy_settings(db: DbClient) -> dict[str, Any]:
    _ensure_proxy_settings_row(db)

    payload = db.query_json(
        """
        SELECT row_to_json(s)
        FROM (
            SELECT
                id,
                proxy_enabled,
                proxy_mode,
                copernicus_via_proxy,
                nasa_via_proxy,
                bypass_hosts,
                bypass_policy,
                proxy_endpoint,
                timeout_seconds,
                max_retries,
                backoff_schedule,
                last_check_at,
                last_check_result,
                last_check_reason,
                last_proxy_latency_ms,
                last_source_latency_ms,
                last_source_status,
                source_reachability,
                updated_at,
                updated_by
            FROM proxy_settings
            WHERE id = 1
        ) s;
        """
    )

    if not isinstance(payload, dict):
        raise Stage4Error("Не удалось загрузить proxy_settings")

    payload["bypass_hosts"] = _parse_json_field(payload.get("bypass_hosts"), [])
    payload["backoff_schedule"] = _parse_json_field(payload.get("backoff_schedule"), DEFAULT_BACKOFF)

    if not isinstance(payload["bypass_hosts"], list):
        payload["bypass_hosts"] = []
    if not isinstance(payload["backoff_schedule"], list) or not payload["backoff_schedule"]:
        payload["backoff_schedule"] = DEFAULT_BACKOFF

    payload["proxy_endpoint_sanitized"] = _sanitize_proxy_endpoint(payload.get("proxy_endpoint"))
    return payload


def _ensure_admin(db: DbClient, admin_email: str) -> int:
    user_id_raw = db.exec_checked(
        f"""
        SELECT u.id
        FROM app_users u
        JOIN roles r ON r.id = u.role_id
        WHERE u.email = {_sql_quote(admin_email)}
          AND u.is_active = TRUE
          AND r.code = 'admin'
        ORDER BY u.id
        LIMIT 1;
        """,
        tuples_only=True,
    )

    if not user_id_raw:
        raise Stage4Error("Доступ запрещён: управление proxy доступно только роли admin")

    return int(user_id_raw)


def ensure_stage4_admin(db: DbClient, admin_email: str) -> int:
    db.exec_checked(
        """
        INSERT INTO roles (code, name)
        VALUES ('admin', 'Администратор')
        ON CONFLICT (code) DO NOTHING;
        """
    )

    db.exec_checked(
        f"""
        INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
        VALUES (
            (SELECT id FROM enterprises ORDER BY id LIMIT 1),
            (SELECT id FROM roles WHERE code = 'admin' LIMIT 1),
            {_sql_quote(admin_email)},
            'Системный администратор',
            'hash',
            TRUE
        )
        ON CONFLICT (email) DO NOTHING;
        """
    )

    return _ensure_admin(db, admin_email)


def set_proxy_settings(
    db: DbClient,
    *,
    admin_email: str,
    enabled: bool,
    mode: str,
    proxy_endpoint: str,
    copernicus_via_proxy: bool,
    nasa_via_proxy: bool,
    bypass_hosts: list[str],
    bypass_policy: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_schedule: list[int],
) -> dict[str, Any]:
    if mode not in {"global", "per_provider"}:
        raise Stage4Error("mode должен быть global или per_provider")
    if bypass_policy not in {"direct", "force_proxy"}:
        raise Stage4Error("bypass_policy должен быть direct или force_proxy")
    if timeout_seconds < 1 or timeout_seconds > 120:
        raise Stage4Error("timeout_seconds должен быть в диапазоне 1..120")
    if max_retries < 0 or max_retries > 10:
        raise Stage4Error("max_retries должен быть в диапазоне 0..10")
    if not backoff_schedule:
        raise Stage4Error("backoff_schedule не может быть пустым")

    admin_id = _ensure_admin(db, admin_email)
    _ensure_proxy_settings_row(db)

    endpoint_sanitized = _sanitize_proxy_endpoint(proxy_endpoint)
    if enabled and not endpoint_sanitized:
        raise Stage4Error("При включенном proxy требуется proxy_endpoint")

    db.exec_checked(
        f"""
        UPDATE proxy_settings
        SET proxy_enabled = {str(enabled).upper()},
            proxy_mode = {_sql_quote(mode)},
            copernicus_via_proxy = {str(copernicus_via_proxy).upper()},
            nasa_via_proxy = {str(nasa_via_proxy).upper()},
            bypass_hosts = {_sql_quote(json.dumps(bypass_hosts, ensure_ascii=False))}::jsonb,
            bypass_policy = {_sql_quote(bypass_policy)},
            proxy_endpoint = {(_sql_quote(endpoint_sanitized) if endpoint_sanitized else 'NULL')},
            timeout_seconds = {int(timeout_seconds)},
            max_retries = {int(max_retries)},
            backoff_schedule = {_sql_quote(json.dumps(backoff_schedule))}::jsonb,
            updated_by = {admin_id},
            updated_at = NOW()
        WHERE id = 1;
        """
    )

    return _format_proxy_settings(_load_proxy_settings(db))


def _format_proxy_settings(settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "proxy_enabled": bool(settings["proxy_enabled"]),
        "proxy_mode": settings["proxy_mode"],
        "copernicus_via_proxy": bool(settings["copernicus_via_proxy"]),
        "nasa_via_proxy": bool(settings["nasa_via_proxy"]),
        "bypass_hosts": settings["bypass_hosts"],
        "bypass_policy": settings["bypass_policy"],
        "proxy_endpoint": settings["proxy_endpoint_sanitized"],
        "timeout_seconds": int(settings["timeout_seconds"]),
        "max_retries": int(settings["max_retries"]),
        "backoff_schedule": settings["backoff_schedule"],
        "last_check_at": settings.get("last_check_at"),
        "last_check_result": settings.get("last_check_result"),
        "last_check_reason": settings.get("last_check_reason"),
        "last_proxy_latency_ms": settings.get("last_proxy_latency_ms"),
        "last_source_latency_ms": settings.get("last_source_latency_ms"),
        "last_source_status": settings.get("last_source_status"),
        "source_reachability": settings.get("source_reachability"),
        "updated_at": settings.get("updated_at"),
        "updated_by": settings.get("updated_by"),
    }


def get_proxy_settings(db: DbClient, *, admin_email: str) -> dict[str, Any]:
    _ensure_admin(db, admin_email)
    return _format_proxy_settings(_load_proxy_settings(db))


def resolve_proxy_usage(
    settings: dict[str, Any],
    *,
    provider: str,
    module_name: str,
    target_host: str,
) -> ProxyDecision:
    _validate_source(provider)

    if not _module_in_proxy_scope(module_name):
        return ProxyDecision(use_proxy=False, reason="module_out_of_scope")

    if not bool(settings["proxy_enabled"]):
        return ProxyDecision(use_proxy=False, reason="proxy_disabled")

    use_proxy = True

    if settings["proxy_mode"] == "per_provider":
        if provider == "Copernicus":
            use_proxy = bool(settings["copernicus_via_proxy"])
        elif provider == "NASA":
            use_proxy = bool(settings["nasa_via_proxy"])
        else:
            use_proxy = False

    bypass_hosts = [str(item) for item in settings.get("bypass_hosts", [])]
    bypass_policy = settings.get("bypass_policy", "direct")

    if any(_host_matches(target_host, host_pattern) for host_pattern in bypass_hosts):
        if bypass_policy == "direct":
            return ProxyDecision(use_proxy=False, reason="bypass_direct")
        return ProxyDecision(use_proxy=True, reason="bypass_force_proxy")

    return ProxyDecision(use_proxy=use_proxy, reason="default_policy")


def _insert_request_log(db: DbClient, outcome: RequestOutcome) -> None:
    db.exec_checked(
        f"""
        INSERT INTO proxy_request_logs (
            request_id,
            provider,
            module_name,
            proxy_used,
            target_host,
            http_status,
            bytes_downloaded,
            duration_ms,
            error_class,
            retry_count,
            success
        )
        VALUES (
            {_sql_quote(outcome.request_id)},
            {_sql_quote(outcome.provider)},
            {_sql_quote(outcome.module_name)},
            {str(outcome.proxy_used).upper()},
            {_sql_quote(outcome.target_host)},
            {str(outcome.http_status) if outcome.http_status is not None else 'NULL'},
            {int(outcome.bytes_downloaded)},
            {int(outcome.duration_ms)},
            {(_sql_quote(outcome.error_class) if outcome.error_class else 'NULL')},
            {int(outcome.retry_count)},
            {str(outcome.success).upper()}
        );
        """
    )


def _update_provider_sync_state(
    db: DbClient,
    *,
    provider: str,
    success: bool,
    reason: str | None,
    proxy_used: bool,
    request_id: str,
) -> None:
    if success:
        db.exec_checked(
            f"""
            INSERT INTO provider_sync_status (source, last_sync_at, last_success_at, status, last_error, updated_at)
            VALUES ({_sql_quote(provider)}, NOW(), NOW(), 'ok', NULL, NOW())
            ON CONFLICT (source)
            DO UPDATE SET
                last_sync_at = NOW(),
                last_success_at = NOW(),
                status = 'ok',
                last_error = NULL,
                updated_at = NOW();
            """
        )
    else:
        db.exec_checked(
            f"""
            INSERT INTO provider_sync_status (source, last_sync_at, last_success_at, status, last_error, updated_at)
            VALUES ({_sql_quote(provider)}, NOW(), NULL, 'error', {_sql_quote((reason or '')[:1500])}, NOW())
            ON CONFLICT (source)
            DO UPDATE SET
                last_sync_at = NOW(),
                status = 'error',
                last_error = EXCLUDED.last_error,
                updated_at = NOW();
            """
        )

    db.exec_checked(
        f"""
        INSERT INTO provider_sync_journal (source, status, reason, proxy_used, request_id)
        VALUES (
            {_sql_quote(provider)},
            {_sql_quote('ok' if success else 'fail')},
            {(_sql_quote(reason[:1500]) if reason else 'NULL')},
            {str(proxy_used).upper()},
            {_sql_quote(request_id)}
        );
        """
    )


def perform_provider_request(
    db: DbClient,
    *,
    provider: str,
    module_name: str,
    url: str,
    request_id: str | None = None,
    update_sync_state: bool = True,
) -> dict[str, Any]:
    _validate_source(provider)

    settings = _load_proxy_settings(db)
    parsed_url = urllib.parse.urlsplit(url)
    target_host = parsed_url.hostname or "unknown"
    request_id_value = request_id or f"req-{uuid.uuid4().hex[:12]}"

    decision = resolve_proxy_usage(
        settings,
        provider=provider,
        module_name=module_name,
        target_host=target_host,
    )

    timeout_seconds = int(settings.get("timeout_seconds", 10))
    max_retries = int(settings.get("max_retries", 3))
    backoff_schedule = [int(x) for x in settings.get("backoff_schedule", DEFAULT_BACKOFF)]

    retry_count = 0

    while True:
        started = time.perf_counter()
        http_status: int | None = None
        bytes_downloaded = 0
        error_class: str | None = None
        error_message: str | None = None
        response_payload = ""

        try:
            handlers: list[Any] = []

            if decision.use_proxy:
                proxy_runtime_url = _proxy_endpoint_with_credentials(settings.get("proxy_endpoint"))
                if not proxy_runtime_url:
                    raise Stage4Error("Proxy включён, но proxy_endpoint не задан")
                handlers.append(
                    urllib.request.ProxyHandler(
                        {
                            "http": proxy_runtime_url,
                            "https": proxy_runtime_url,
                        }
                    )
                )
            else:
                handlers.append(urllib.request.ProxyHandler({}))

            opener = urllib.request.build_opener(*handlers)
            request = urllib.request.Request(url=url, method="GET", headers={"User-Agent": "zemledar-stage4/1.0"})

            with opener.open(request, timeout=timeout_seconds) as response:
                http_status = int(getattr(response, "status", 200))
                body = response.read()
                bytes_downloaded = len(body)
                response_payload = body.decode("utf-8", errors="replace")[:4000]

            duration_ms = int((time.perf_counter() - started) * 1000)

            outcome = RequestOutcome(
                success=True,
                provider=provider,
                module_name=module_name,
                request_id=request_id_value,
                target_host=target_host,
                proxy_used=decision.use_proxy,
                retry_count=retry_count,
                duration_ms=duration_ms,
                bytes_downloaded=bytes_downloaded,
                http_status=http_status,
                error_class=None,
                error_message=None,
            )
            _insert_request_log(db, outcome)

            if update_sync_state:
                _update_provider_sync_state(
                    db,
                    provider=provider,
                    success=True,
                    reason=None,
                    proxy_used=decision.use_proxy,
                    request_id=request_id_value,
                )

            return {
                "success": True,
                "request_id": request_id_value,
                "provider": provider,
                "module_name": module_name,
                "proxy_used": decision.use_proxy,
                "proxy_reason": decision.reason,
                "target_host": target_host,
                "http_status": http_status,
                "duration_ms": duration_ms,
                "bytes_downloaded": bytes_downloaded,
                "retry_count": retry_count,
                "error_class": None,
                "error_message": None,
                "response_preview": response_payload,
            }
        except urllib.error.HTTPError as exc:
            http_status = int(exc.code)
            error_class = _classify_exception(exc, http_status=http_status)
            error_message = f"HTTP {exc.code}"
            body = exc.read()
            bytes_downloaded = len(body)
        except Exception as exc:  # noqa: BLE001
            error_class = _classify_exception(exc)
            error_message = _sanitize_text(str(exc))

        duration_ms = int((time.perf_counter() - started) * 1000)

        can_retry = _is_retryable(error_class or "unknown", http_status, retry_count, max_retries)
        if can_retry:
            _backoff_sleep(backoff_schedule, retry_count)
            retry_count += 1
            continue

        outcome = RequestOutcome(
            success=False,
            provider=provider,
            module_name=module_name,
            request_id=request_id_value,
            target_host=target_host,
            proxy_used=decision.use_proxy,
            retry_count=retry_count,
            duration_ms=duration_ms,
            bytes_downloaded=bytes_downloaded,
            http_status=http_status,
            error_class=error_class,
            error_message=error_message,
        )
        _insert_request_log(db, outcome)

        if update_sync_state:
            _update_provider_sync_state(
                db,
                provider=provider,
                success=False,
                reason=f"{error_class or 'unknown'}: {error_message or 'ошибка'}",
                proxy_used=decision.use_proxy,
                request_id=request_id_value,
            )

        return {
            "success": False,
            "request_id": request_id_value,
            "provider": provider,
            "module_name": module_name,
            "proxy_used": decision.use_proxy,
            "proxy_reason": decision.reason,
            "target_host": target_host,
            "http_status": http_status,
            "duration_ms": duration_ms,
            "bytes_downloaded": bytes_downloaded,
            "retry_count": retry_count,
            "error_class": error_class,
            "error_message": error_message,
            "response_preview": "",
        }


def _check_proxy_connectivity(settings: dict[str, Any], timeout_seconds: int) -> ProxyCheckOutcome:
    endpoint = settings.get("proxy_endpoint")
    if not endpoint:
        return ProxyCheckOutcome(
            status="FAIL",
            latency_ms=None,
            error_class="config",
            reason="proxy_endpoint не задан",
        )

    parsed = urllib.parse.urlsplit(str(endpoint))
    host = parsed.hostname
    if not host:
        return ProxyCheckOutcome(status="FAIL", latency_ms=None, error_class="config", reason="Неверный proxy_endpoint")

    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == "https" else 80

    started = time.perf_counter()

    try:
        with socket.create_connection((host, int(port)), timeout=timeout_seconds) as sock:
            if parsed.scheme == "https":
                context = ssl.create_default_context()
                with context.wrap_socket(sock, server_hostname=host) as tls_sock:
                    tls_sock.do_handshake()
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProxyCheckOutcome(status="OK", latency_ms=latency_ms, error_class=None, reason="Прокси доступен")
    except Exception as exc:  # noqa: BLE001
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProxyCheckOutcome(
            status="FAIL",
            latency_ms=latency_ms,
            error_class=_classify_exception(exc),
            reason=_sanitize_text(str(exc)),
        )


def run_proxy_health_check(
    db: DbClient,
    *,
    admin_email: str,
    provider: str,
    module_name: str,
    source_url: str,
) -> dict[str, Any]:
    _validate_source(provider)
    _ensure_admin(db, admin_email)

    settings = _load_proxy_settings(db)
    decision = resolve_proxy_usage(
        settings,
        provider=provider,
        module_name=module_name,
        target_host=urllib.parse.urlsplit(source_url).hostname or "unknown",
    )

    proxy_enabled = bool(settings.get("proxy_enabled"))
    timeout_seconds = int(settings.get("timeout_seconds", 10))

    if proxy_enabled and decision.use_proxy:
        proxy_check = _check_proxy_connectivity(settings, timeout_seconds)
    else:
        proxy_check = ProxyCheckOutcome(
            status="OK",
            latency_ms=0,
            error_class=None,
            reason="Прокси не используется для текущего запроса",
        )

    source_result = perform_provider_request(
        db,
        provider=provider,
        module_name=module_name,
        url=source_url,
        request_id=f"health-{uuid.uuid4().hex[:10]}",
        update_sync_state=False,
    )

    source_reachability = "OK" if source_result["success"] else "FAIL"
    last_check_result = "OK" if (proxy_check.status == "OK" and source_result["success"]) else "FAIL"
    reason_parts: list[str] = []

    if proxy_check.status != "OK":
        reason_parts.append(f"proxy={proxy_check.error_class or 'error'}:{proxy_check.reason}")
    if not source_result["success"]:
        reason_parts.append(
            f"source={source_result.get('error_class') or 'error'}:{source_result.get('error_message') or 'unreachable'}"
        )

    reason_text = "; ".join(reason_parts) if reason_parts else "Проверка соединения успешна"

    db.exec_checked(
        f"""
        UPDATE proxy_settings
        SET last_check_at = NOW(),
            last_check_result = {_sql_quote(last_check_result)},
            last_check_reason = {_sql_quote(reason_text[:1500])},
            last_proxy_latency_ms = {str(proxy_check.latency_ms) if proxy_check.latency_ms is not None else 'NULL'},
            last_source_latency_ms = {int(source_result['duration_ms'])},
            last_source_status = {str(source_result['http_status']) if source_result['http_status'] is not None else 'NULL'},
            source_reachability = {_sql_quote(source_reachability)},
            updated_at = NOW()
        WHERE id = 1;
        """
    )

    return {
        "proxy_enabled": proxy_enabled,
        "proxy_used_for_source": decision.use_proxy,
        "proxy_last_check_at": _now_utc_iso(),
        "proxy_check_result": {
            "status": proxy_check.status,
            "error_class": proxy_check.error_class,
            "reason": proxy_check.reason,
            "latency_ms": proxy_check.latency_ms,
        },
        "source_reachability": {
            "status": source_reachability,
            "http_status": source_result.get("http_status"),
            "duration_ms": source_result.get("duration_ms"),
            "error_class": source_result.get("error_class"),
            "reason": source_result.get("error_message"),
        },
    }


def get_degradation_status(db: DbClient, provider: str) -> dict[str, Any]:
    _validate_source(provider)

    payload = db.query_json(
        f"""
        SELECT COALESCE((
            SELECT row_to_json(s)
            FROM (
                SELECT
                    source,
                    status,
                    to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_sync_at,
                    to_char(last_success_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_success_at,
                    last_error
                FROM provider_sync_status
                WHERE source = {_sql_quote(provider)}
            ) s
        ), '{{"source": "{provider}", "status": "never"}}'::json);
        """
    )

    if not isinstance(payload, dict):
        raise Stage4Error("Не удалось получить статус деградации")

    if payload.get("status") == "error":
        payload["degradation_mode"] = True
        payload["message"] = (
            "Источник недоступен. Показаны данные последней успешной синхронизации. "
            f"Причина: {payload.get('last_error') or 'неизвестно'}"
        )
    else:
        payload["degradation_mode"] = False
        payload["message"] = "Источник доступен"

    return payload


def get_proxy_metrics(db: DbClient, *, admin_email: str) -> dict[str, Any]:
    _ensure_admin(db, admin_email)
    rows = db.query_json(
        """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                provider,
                COUNT(*)::int AS total_requests,
                COUNT(*) FILTER (WHERE success = TRUE)::int AS success_requests,
                COUNT(*) FILTER (WHERE success = FALSE)::int AS failed_requests,
                ROUND(AVG(duration_ms)::numeric, 2) AS avg_duration_ms
            FROM proxy_request_logs
            GROUP BY provider
            ORDER BY provider
        ) x;
        """
    )

    error_rows = db.query_json(
        """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                provider,
                COALESCE(error_class, 'none') AS error_class,
                COUNT(*)::int AS total
            FROM proxy_request_logs
            GROUP BY provider, COALESCE(error_class, 'none')
            ORDER BY provider, error_class
        ) x;
        """
    )

    assert isinstance(rows, list)
    assert isinstance(error_rows, list)

    return {
        "by_provider": rows,
        "errors_by_class": error_rows,
    }


def get_request_log(db: DbClient, request_id: str, *, admin_email: str) -> dict[str, Any]:
    _ensure_admin(db, admin_email)
    payload = db.query_json(
        f"""
        SELECT COALESCE((
            SELECT row_to_json(s)
            FROM (
                SELECT
                    request_id,
                    provider,
                    module_name,
                    proxy_used,
                    target_host,
                    http_status,
                    bytes_downloaded,
                    duration_ms,
                    error_class,
                    retry_count,
                    success,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM proxy_request_logs
                WHERE request_id = {_sql_quote(request_id)}
                ORDER BY id DESC
                LIMIT 1
            ) s
        ), '{{"request_id": "{request_id}", "found": false}}'::json);
        """
    )

    if not isinstance(payload, dict):
        raise Stage4Error("Не удалось получить лог запроса")

    if "found" in payload:
        return payload

    payload["found"] = True
    return payload
