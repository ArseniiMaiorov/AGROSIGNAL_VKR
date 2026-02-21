from datetime import datetime, timezone

from internal.app.config import AppConfig


def build_health_payload(config: AppConfig) -> dict[str, str]:
    return {
        "status": "ok",
        "service": config.app_name,
        "environment": config.app_env,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
