import os
from pathlib import Path
import sys

from internal.app.config import AppConfig, load_config

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from stage5_api import create_server as create_stage5_server


def build_app_config() -> AppConfig:
    return load_config()


def create_server(host: str = "0.0.0.0", port: int = 8000) -> object:
    return create_stage5_server(build_app_config(), host=host, port=port)


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
