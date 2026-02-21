from __future__ import annotations

import json
import os
import socketserver
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Callable, Iterator

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "reports" / "tests" / f"{date.today().isoformat()}_stage4_proxy.md"
ADMIN_EMAIL = "admin.stage4@zemledar.local"


@dataclass
class CaseResult:
    feature: str
    input_data: str
    expected: str
    actual: str
    status: str


@dataclass
class ServerRef:
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class ThreadingHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextmanager
def run_http_server(
    callback: Callable[[str, dict[str, str], bytes], tuple[int, dict[str, str], bytes]],
) -> Iterator[ServerRef]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            self._handle()

        def do_POST(self) -> None:  # noqa: N802
            self._handle()

        def do_CONNECT(self) -> None:  # noqa: N802
            self.send_response(501)
            self.end_headers()

        def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003
            return

        def _handle(self) -> None:
            content_len = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_len) if content_len > 0 else b""
            headers = {key.lower(): value for key, value in self.headers.items()}
            status, extra_headers, payload = callback(self.path, headers, body)

            self.send_response(status)
            for key, value in extra_headers.items():
                self.send_header(key, value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        host, port = server.server_address[0], server.server_address[1]
        yield ServerRef(host=host, port=int(port))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def run_cli(*args: str, env: dict[str, str] | None = None) -> dict[str, Any]:
    cmd = [sys.executable, "scripts/stage4_cli.py", *args]
    run_env = os.environ.copy()
    run_env["STAGE4_FAST_MODE"] = "1"
    if env:
        run_env.update(env)

    result = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        capture_output=True,
        env=run_env,
        check=False,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Команда {' '.join(args)} завершилась ошибкой: {stderr}")

    stdout = result.stdout.strip()
    if not stdout:
        return {}
    return json.loads(stdout)


def add_case(cases: list[CaseResult], feature: str, input_data: str, expected: str, ok: bool, actual: str) -> None:
    cases.append(
        CaseResult(
            feature=feature,
            input_data=input_data,
            expected=expected,
            actual=actual,
            status="PASS" if ok else "FAIL",
        )
    )


def write_report(cases: list[CaseResult]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["# Протокол тестирования этапа 4: proxy-контур, ретраи, наблюдаемость", ""]

    for case in cases:
        lines.extend(
            [
                f"Фича: {case.feature}",
                "Вход:",
                f"  - {case.input_data}",
                "Ожидаемый результат:",
                f"  - {case.expected}",
                "Фактический результат:",
                f"  - {case.actual}",
                f"Статус: {case.status}",
                "",
            ]
        )

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def configure_proxy(
    *,
    enabled: bool,
    mode: str,
    endpoint: str,
    copernicus_via_proxy: bool = True,
    nasa_via_proxy: bool = True,
    bypass_hosts: list[str] | None = None,
    bypass_policy: str = "direct",
    timeout_seconds: int = 3,
    max_retries: int = 3,
    backoff: str = "1,5,15",
) -> dict[str, Any]:
    args = [
        "proxy-set",
        "--admin-email",
        ADMIN_EMAIL,
        "--enabled",
        "true" if enabled else "false",
        "--mode",
        mode,
        "--proxy-endpoint",
        endpoint,
        "--copernicus-via-proxy",
        "true" if copernicus_via_proxy else "false",
        "--nasa-via-proxy",
        "true" if nasa_via_proxy else "false",
        "--bypass-policy",
        bypass_policy,
        "--timeout-seconds",
        str(timeout_seconds),
        "--max-retries",
        str(max_retries),
        "--backoff",
        backoff,
    ]

    if bypass_hosts:
        for host in bypass_hosts:
            args.extend(["--bypass-host", host])
    else:
        args.append("--clear-bypass")

    return run_cli(*args)


def main() -> int:
    cases: list[CaseResult] = []

    try:
        run_cli("ensure-admin", "--email", ADMIN_EMAIL)
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Инициализация администратора",
            input_data=f"ensure-admin --email {ADMIN_EMAIL}",
            expected="Админ создан и может управлять proxy",
            ok=False,
            actual=str(exc),
        )
        write_report(cases)
        print(f"Сформирован отчёт: {REPORT_PATH.relative_to(ROOT)}")
        return 1

    # 1) Proxy ON + invalid credentials -> 401 без ретраев
    try:
        with run_http_server(
            lambda _path, _headers, _body: (401, {}, b'{"error":"invalid proxy credentials"}')
        ) as proxy_server:
            configure_proxy(
                enabled=True,
                mode="global",
                endpoint=proxy_server.base_url,
                copernicus_via_proxy=True,
                nasa_via_proxy=True,
                bypass_hosts=[],
                max_retries=3,
            )

            result = run_cli(
                "request",
                "--provider",
                "Copernicus",
                "--module",
                "providers/copernicus/sync",
                "--url",
                "http://provider.local/ping",
                "--request-id",
                "stage4-case401",
            )

        retry_count = result.get("retry_count")
        ok = (
            not bool(result.get("success"))
            and int(result.get("http_status") or 0) == 401
            and result.get("error_class") == "auth"
            and (retry_count == 0 or retry_count == "0")
        )
        add_case(
            cases,
            feature="Proxy ON + неверные креды",
            input_data="proxy_enabled=true, source=Copernicus, proxy отвечает 401",
            expected="FAIL, error_class=auth, retry_count=0",
            ok=ok,
            actual=(
                f"success={result.get('success')}, http_status={result.get('http_status')}, "
                f"error_class={result.get('error_class')}, retry_count={result.get('retry_count')}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Proxy ON + неверные креды",
            input_data="proxy_enabled=true, source=Copernicus, proxy отвечает 401",
            expected="FAIL, error_class=auth, retry_count=0",
            ok=False,
            actual=str(exc),
        )

    # 2) Proxy ON + DNS error
    try:
        configure_proxy(
            enabled=True,
            mode="global",
            endpoint="http://nonexistent-proxy.invalid:3128",
            copernicus_via_proxy=True,
            nasa_via_proxy=True,
            bypass_hosts=[],
            max_retries=3,
        )
        result = run_cli(
            "request",
            "--provider",
            "Copernicus",
            "--module",
            "providers/copernicus/sync",
            "--url",
            "http://localhost/ping",
            "--request-id",
            "stage4-casedns",
        )

        ok = (not bool(result.get("success"))) and result.get("error_class") == "dns"
        add_case(
            cases,
            feature="Proxy ON + DNS ошибка",
            input_data="proxy_endpoint=nonexistent-proxy.invalid, запрос Copernicus",
            expected="FAIL, error_class=dns",
            ok=ok,
            actual=f"success={result.get('success')}, error_class={result.get('error_class')}",
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Proxy ON + DNS ошибка",
            input_data="proxy_endpoint=nonexistent-proxy.invalid, запрос Copernicus",
            expected="FAIL, error_class=dns",
            ok=False,
            actual=str(exc),
        )

    # 3) Мягкий фоллбек/деградация после ошибки
    try:
        degradation = run_cli("degradation-status", "--provider", "Copernicus")
        ok = bool(degradation.get("degradation_mode")) and "Источник недоступен" in str(degradation.get("message", ""))
        add_case(
            cases,
            feature="Режим деградации",
            input_data="degradation-status --provider Copernicus после failed sync",
            expected="degradation_mode=true, понятная причина для UI",
            ok=ok,
            actual=(
                f"status={degradation.get('status')}, degradation_mode={degradation.get('degradation_mode')}, "
                f"message={degradation.get('message')}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Режим деградации",
            input_data="degradation-status --provider Copernicus после failed sync",
            expected="degradation_mode=true, понятная причина для UI",
            ok=False,
            actual=str(exc),
        )

    # 4) Proxy ON + TLS error
    try:
        with run_http_server(lambda _path, _headers, _body: (200, {}, b'{"status":"ok"}')) as source_server:
            configure_proxy(
                enabled=True,
                mode="global",
                endpoint="http://nonexistent-proxy.invalid:3128",
                copernicus_via_proxy=True,
                nasa_via_proxy=True,
                bypass_hosts=["127.0.0.1"],
                max_retries=2,
            )

            result = run_cli(
                "request",
                "--provider",
                "Copernicus",
                "--module",
                "providers/copernicus/sync",
                "--url",
                f"https://127.0.0.1:{source_server.port}/tls",
                "--request-id",
                "stage4-casetls",
            )

        ok = (not bool(result.get("success"))) and result.get("error_class") == "tls"
        add_case(
            cases,
            feature="Proxy ON + TLS ошибка сертификата",
            input_data="proxy ON + bypass 127.0.0.1 + запрос https к http-источнику",
            expected="FAIL, error_class=tls",
            ok=ok,
            actual=f"success={result.get('success')}, error_class={result.get('error_class')}",
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Proxy ON + TLS ошибка сертификата",
            input_data="proxy ON + bypass 127.0.0.1 + запрос https к http-источнику",
            expected="FAIL, error_class=tls",
            ok=False,
            actual=str(exc),
        )

    # 5) Proxy ON + 429 retry policy
    try:
        state = {"calls": 0}

        def proxy_429(_path: str, _headers: dict[str, str], _body: bytes) -> tuple[int, dict[str, str], bytes]:
            state["calls"] += 1
            if state["calls"] <= 2:
                return (429, {"Retry-After": "1"}, b'{"error":"too many requests"}')
            return (200, {}, b'{"ok":true}')

        with run_http_server(proxy_429) as proxy_server:
            configure_proxy(
                enabled=True,
                mode="global",
                endpoint=proxy_server.base_url,
                copernicus_via_proxy=True,
                nasa_via_proxy=True,
                bypass_hosts=[],
                max_retries=3,
                backoff="1,5,15",
            )
            result = run_cli(
                "request",
                "--provider",
                "Copernicus",
                "--module",
                "providers/copernicus/sync",
                "--url",
                "http://provider.local/rate-limit",
                "--request-id",
                "stage4-case429",
            )

        ok = bool(result.get("success")) and int(result.get("retry_count") or -1) == 2 and state["calls"] == 3
        add_case(
            cases,
            feature="Proxy ON + источник 429",
            input_data="proxy отвечает 429,429,200; max_retries=3",
            expected="PASS после ретраев (retry_count=2) либо контролируемый FAIL",
            ok=ok,
            actual=(
                f"success={result.get('success')}, retry_count={result.get('retry_count')}, "
                f"http_status={result.get('http_status')}, calls={state['calls']}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Proxy ON + источник 429",
            input_data="proxy отвечает 429,429,200; max_retries=3",
            expected="PASS после ретраев (retry_count=2) либо контролируемый FAIL",
            ok=False,
            actual=str(exc),
        )

    # 6) Per-provider режим: Copernicus через proxy, NASA напрямую
    cop_request_id = "stage4-perprovider-cop"
    nasa_request_id = "stage4-perprovider-nasa"
    try:
        with run_http_server(lambda _path, _headers, _body: (200, {}, b'{"via":"proxy"}')) as proxy_server:
            with run_http_server(lambda _path, _headers, _body: (200, {}, b'{"via":"direct"}')) as source_server:
                configure_proxy(
                    enabled=True,
                    mode="per_provider",
                    endpoint=proxy_server.base_url,
                    copernicus_via_proxy=True,
                    nasa_via_proxy=False,
                    bypass_hosts=[],
                    max_retries=3,
                )

                cop_result = run_cli(
                    "request",
                    "--provider",
                    "Copernicus",
                    "--module",
                    "providers/copernicus/sync",
                    "--url",
                    "http://provider.local/per-provider",
                    "--request-id",
                    cop_request_id,
                )
                nasa_result = run_cli(
                    "request",
                    "--provider",
                    "NASA",
                    "--module",
                    "providers/nasa/sync",
                    "--url",
                    f"{source_server.base_url}/nasa",
                    "--request-id",
                    nasa_request_id,
                )

        ok = (
            bool(cop_result.get("success"))
            and bool(nasa_result.get("success"))
            and bool(cop_result.get("proxy_used"))
            and not bool(nasa_result.get("proxy_used"))
        )
        add_case(
            cases,
            feature="Per-provider режим",
            input_data="mode=per_provider, Copernicus=true, NASA=false",
            expected="Copernicus -> proxy_used=true, NASA -> proxy_used=false, оба PASS",
            ok=ok,
            actual=(
                f"cop_success={cop_result.get('success')}, cop_proxy={cop_result.get('proxy_used')}; "
                f"nasa_success={nasa_result.get('success')}, nasa_proxy={nasa_result.get('proxy_used')}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Per-provider режим",
            input_data="mode=per_provider, Copernicus=true, NASA=false",
            expected="Copernicus -> proxy_used=true, NASA -> proxy_used=false, оба PASS",
            ok=False,
            actual=str(exc),
        )

    # 7) Bypass-list: домен идёт напрямую при proxy ON
    try:
        with run_http_server(lambda _path, _headers, _body: (200, {}, b'{"via":"direct-by-bypass"}')) as source_server:
            configure_proxy(
                enabled=True,
                mode="global",
                endpoint="http://nonexistent-proxy.invalid:3128",
                copernicus_via_proxy=True,
                nasa_via_proxy=True,
                bypass_hosts=["localhost"],
                bypass_policy="direct",
                max_retries=2,
            )

            result = run_cli(
                "request",
                "--provider",
                "Copernicus",
                "--module",
                "providers/copernicus/sync",
                "--url",
                f"http://localhost:{source_server.port}/bypass",
                "--request-id",
                "stage4-bypass",
            )

        ok = bool(result.get("success")) and not bool(result.get("proxy_used"))
        add_case(
            cases,
            feature="Bypass list",
            input_data="proxy ON + bypass_hosts=localhost + proxy недоступен",
            expected="Запрос к localhost выполняется напрямую (proxy_used=false) и PASS",
            ok=ok,
            actual=f"success={result.get('success')}, proxy_used={result.get('proxy_used')}",
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Bypass list",
            input_data="proxy ON + bypass_hosts=localhost + proxy недоступен",
            expected="Запрос к localhost выполняется напрямую (proxy_used=false) и PASS",
            ok=False,
            actual=str(exc),
        )

    # 8) Логи: proxy_used=true/false фиксируется корректно
    try:
        cop_log = run_cli("request-log", "--request-id", cop_request_id)
        nasa_log = run_cli("request-log", "--request-id", nasa_request_id)
        ok = (
            bool(cop_log.get("found"))
            and bool(nasa_log.get("found"))
            and bool(cop_log.get("proxy_used"))
            and not bool(nasa_log.get("proxy_used"))
        )
        add_case(
            cases,
            feature="Логирование proxy_used",
            input_data=f"request-log для {cop_request_id} и {nasa_request_id}",
            expected="В логе Copernicus proxy_used=true, NASA proxy_used=false",
            ok=ok,
            actual=f"cop_proxy={cop_log.get('proxy_used')}, nasa_proxy={nasa_log.get('proxy_used')}",
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Логирование proxy_used",
            input_data=f"request-log для {cop_request_id} и {nasa_request_id}",
            expected="В логе Copernicus proxy_used=true, NASA proxy_used=false",
            ok=False,
            actual=str(exc),
        )

    # 9) Health-check: структура статуса proxy/source
    try:
        with run_http_server(lambda _path, _headers, _body: (200, {}, b'{"status":"ok"}')) as source_server:
            configure_proxy(
                enabled=False,
                mode="global",
                endpoint="http://127.0.0.1:3128",
                copernicus_via_proxy=True,
                nasa_via_proxy=True,
                bypass_hosts=[],
            )
            result = run_cli(
                "health-check",
                "--admin-email",
                ADMIN_EMAIL,
                "--provider",
                "NASA",
                "--module",
                "providers/nasa/sync",
                "--source-url",
                f"{source_server.base_url}/health",
            )

        ok = (
            "proxy_enabled" in result
            and "proxy_last_check_at" in result
            and "proxy_check_result" in result
            and "source_reachability" in result
            and result.get("source_reachability", {}).get("status") == "OK"
        )
        add_case(
            cases,
            feature="Health-check proxy/source",
            input_data="health-check --provider NASA --module providers/nasa/sync",
            expected="proxy_enabled/proxy_last_check_at/proxy_check_result/source_reachability в ответе",
            ok=ok,
            actual=(
                f"proxy_enabled={result.get('proxy_enabled')}, "
                f"proxy_result={result.get('proxy_check_result', {}).get('status')}, "
                f"source_status={result.get('source_reachability', {}).get('status')}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        add_case(
            cases,
            feature="Health-check proxy/source",
            input_data="health-check --provider NASA --module providers/nasa/sync",
            expected="proxy_enabled/proxy_last_check_at/proxy_check_result/source_reachability в ответе",
            ok=False,
            actual=str(exc),
        )

    write_report(cases)
    print(f"Сформирован отчёт: {REPORT_PATH.relative_to(ROOT)}")

    failed = [case for case in cases if case.status == "FAIL"]
    if failed:
        for case in failed:
            print(f"FAIL: {case.feature} -> {case.actual}")
        return 1

    for case in cases:
        print(f"PASS: {case.feature}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
