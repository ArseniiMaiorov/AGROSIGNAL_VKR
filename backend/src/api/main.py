import json
import os
import socketserver
from http import HTTPStatus

from internal.app.config import AppConfig, load_config
from internal.app.health import build_health_payload


def build_app_config() -> AppConfig:
    return load_config()


class HealthRequestHandler(socketserver.StreamRequestHandler):
    config = build_app_config()

    def handle(self) -> None:
        self.connection.settimeout(3)

        request_line_raw = self.rfile.readline(65536)
        if not request_line_raw:
            return

        request_line = request_line_raw.decode("iso-8859-1", errors="replace").strip()

        while True:
            header_line = self.rfile.readline(65536)
            if not header_line or header_line in (b"\r\n", b"\n"):
                break

        if request_line.startswith("GET /health "):
            payload = build_health_payload(self.config)
            body = json.dumps(payload).encode("utf-8")
            response = self._build_response(HTTPStatus.OK, body)
        else:
            payload = {"error": "Маршрут не найден", "status": HTTPStatus.NOT_FOUND}
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            response = self._build_response(HTTPStatus.NOT_FOUND, body)

        self.wfile.write(response)
        self.wfile.flush()

    @staticmethod
    def _build_response(status: HTTPStatus, body: bytes) -> bytes:
        headers = [
            f"HTTP/1.1 {status.value} {status.phrase}",
            "Content-Type: application/json; charset=utf-8",
            f"Content-Length: {len(body)}",
            "Connection: close",
            "",
            "",
        ]
        head = "\r\n".join(headers).encode("ascii")
        return head + body


def create_server(host: str = "0.0.0.0", port: int = 8000) -> socketserver.TCPServer:
    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    return ReusableTCPServer((host, port), HealthRequestHandler)


def resolve_server_bind() -> tuple[str, int]:
    host = os.environ.get("API_HOST", "0.0.0.0")
    port_raw = os.environ.get("API_PORT", "8000")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError(f"Некорректное значение API_PORT: {port_raw}") from exc

    if port < 1 or port > 65535:
        raise RuntimeError("API_PORT должен быть в диапазоне 1..65535")

    return host, port


def main() -> None:
    host, port = resolve_server_bind()
    server = create_server(host=host, port=port)
    server.serve_forever()
