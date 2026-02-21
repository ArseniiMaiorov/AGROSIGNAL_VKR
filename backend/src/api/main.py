import json
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


def main() -> None:
    server = create_server()
    server.serve_forever()
