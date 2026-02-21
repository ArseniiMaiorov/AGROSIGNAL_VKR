from collections.abc import Mapping
from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class AppConfig:
    app_name: str = "zemledar-api"
    app_env: str = "dev"
    log_level: str = "INFO"

    @property
    def is_production(self) -> bool:
        return self.app_env == "prod"


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    source = os.environ if env is None else env
    return AppConfig(
        app_name=source.get("APP_NAME", "zemledar-api"),
        app_env=source.get("APP_ENV", "dev"),
        log_level=source.get("LOG_LEVEL", "INFO"),
    )
