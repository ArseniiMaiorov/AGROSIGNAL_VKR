from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from internal.app.config import AppConfig
from internal.app.health import build_health_payload
from stage3_cli import (
    CONTRACT_VERSION,
    DbClient,
    Stage3Error,
    _iso_utc,
    _parse_ts,
    _sql_quote,
    create_export_task,
    extend_dataset_ttl,
    get_export_status,
    get_sync_status,
    process_exports,
    query_range,
    run_sync,
)
from stage4_proxy import get_degradation_status

EXPORTS_DIR = ROOT / "exports"
API_VERSION = "v1"
WEATHER_METRICS = {
    "precipitation",
    "temperature",
    "humidity_rh",
    "wind_speed",
    "cloudiness",
    "cloud_total",
    "pressure_msl",
    "radiation",
}
SOIL_METRICS = {"soil_moisture"}
SATELLITE_METRICS = {"ndvi", "ndre", "ndmi"}
SATELLITE_QUALITY_METRICS = {"cloudiness", "cloud_total", "cloud_mask"}
ROLE_ADMIN = "admin"
ROLE_MANAGER = "manager"
ROLE_AGRONOMIST = "agronomist"
ROLE_VIEWER = "viewer"
ROLE_ALIASES = {
    "admin": "admin",
    "manager": "manager",
    "agronomist": "agronomist",
    "agronom": "agronomist",
    "viewer": "viewer",
}
SOURCE_ALIASES = {
    "copernicus": "Copernicus",
    "nasa": "NASA",
    "mock": "Mock",
}
LAYER_METRIC_MAP = {
    # Stage5 legacy aliases
    "weather.wind_vector_10m": ("wind_speed",),
    "weather.temperature_2m": ("temperature",),
    "weather.precipitation": ("precipitation",),
    "satellite.ndvi": ("ndvi",),
    "satellite.ndre": ("ndre",),
    "satellite.ndmi": ("ndmi",),
    "satellite.cloud_mask": ("cloud_mask", "cloudiness"),
    # Stage6 canonical layers
    "weather.temp_2m": ("temperature",),
    "weather.humidity_rh": ("humidity_rh",),
    "weather.precip_sum": ("precipitation",),
    "weather.cloud_total": ("cloud_total", "cloudiness"),
    "weather.pressure_msl": ("pressure_msl",),
    "weather.radiation": ("radiation",),
    "weather.wind_speed_10m": ("wind_speed",),
    "weather.wind_streamlines": ("wind_speed",),
    "weather.vorticity_index": ("wind_speed",),
    "soil.moisture": ("soil_moisture",),
    "soil.moisture_anomaly": ("soil_moisture",),
    "soil.trafficability_risk": ("soil_moisture", "precipitation"),
    "sat.ndvi": ("ndvi",),
    "sat.ndre": ("ndre",),
    "sat.ndmi": ("ndmi",),
    "sat.scene_quality": ("cloud_mask", "cloud_total", "cloudiness"),
    "sat.cloud_mask": ("cloud_mask", "cloud_total", "cloudiness"),
    "sat.season_curve": ("ndvi",),
    "sat.growth_rate": ("ndvi",),
    "sat.field_uniformity_cv": ("ndvi",),
    "sat.anomaly_vs_baseline": ("ndvi",),
}
SCALAR_AGGREGATIONS = {"mean", "sum", "min", "max", "p10", "p90", "median"}
EXPECTED_UNITS: dict[str, str] = {
    "temperature": "C",
    "humidity_rh": "%",
    "wind_speed": "m/s",
    "precipitation": "mm",
    "cloudiness": "%",
    "cloud_total": "%",
    "pressure_msl": "hPa",
    "radiation": "W/m2",
    "soil_moisture": "%",
}
ALGORITHM_VERSION = "algorithms.v1"


class ApiError(RuntimeError):
    def __init__(self, code: str, message: str, *, status: int, details: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details


@dataclass
class UserContext:
    user_id: int
    enterprise_id: int | None
    role_code: str
    email: str
    full_name: str


@dataclass
class ApiHttpResponse:
    status_code: int
    body: bytes
    content_type: str = "application/json; charset=utf-8"
    headers: dict[str, str] = field(default_factory=dict)
    error_code: str | None = None
    user_id: int | None = None


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class Stage5ApiApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._seed_lock = threading.Lock()
        self._seed_done = False

    # -------------------------------
    # Public request handling
    # -------------------------------
    def handle_request(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, list[str]],
        headers: dict[str, str],
        raw_body: bytes,
        request_id: str,
    ) -> ApiHttpResponse:
        if method == "GET" and path == "/health":
            payload = build_health_payload(self.config)
            return ApiHttpResponse(status_code=200, body=self._json_bytes(payload))

        if path == "/api/v1/health" and method == "GET":
            payload = build_health_payload(self.config)
            return self._success(payload, request_id=request_id)

        if not path.startswith("/api/v1/"):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)

        db = DbClient()
        try:
            db.ensure_ready()
            self._ensure_stage5_seed(db)
        except Stage3Error as exc:
            raise ApiError(
                "SOURCE_UNAVAILABLE",
                "Источник данных недоступен: не удалось подключиться к БД",
                status=503,
                details=str(exc),
            ) from exc

        user = self._resolve_user(db, headers)

        # Auth and utility routes
        if path == "/api/v1/auth/me" and method == "GET":
            return self._success(
                {
                    "user_id": user.user_id,
                    "email": user.email,
                    "full_name": user.full_name,
                    "role": user.role_code,
                    "enterprise_id": user.enterprise_id,
                },
                request_id=request_id,
                user_id=user.user_id,
            )

        if path == "/api/v1/metrics/overview" and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            data = self._build_metrics_overview(db)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        if path == "/api/v1/audit" and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            data = self._list_audit_log(db, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        # Enterprises
        if path == "/api/v1/enterprises":
            if method == "GET":
                data = self._list_enterprises(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                payload = self._parse_json_body(raw_body)
                created = self._create_enterprise(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        enterprise_id = self._match_id(path, r"^/api/v1/enterprises/(\d+)$")
        if enterprise_id is not None:
            if method == "GET":
                data = self._get_enterprise(db, user, enterprise_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "PUT":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                payload = self._parse_json_body(raw_body)
                updated = self._update_enterprise(db, user, enterprise_id, payload, request_id)
                return self._success(updated, request_id=request_id, user_id=user.user_id)

        bind_enterprise_id = self._match_id(path, r"^/api/v1/enterprises/(\d+)/users/bind$")
        if bind_enterprise_id is not None and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            payload = self._parse_json_body(raw_body)
            bound = self._bind_user_to_enterprise(db, user, bind_enterprise_id, payload, request_id)
            return self._success(bound, request_id=request_id, user_id=user.user_id)

        # Users
        if path == "/api/v1/users":
            if method == "GET":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                data = self._list_users(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                payload = self._parse_json_body(raw_body)
                created = self._create_user(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        # Fields
        if path == "/api/v1/fields":
            if method == "GET":
                data = self._list_fields(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                created = self._create_field(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        field_id = self._match_id(path, r"^/api/v1/fields/(\d+)$")
        if field_id is not None:
            if method == "GET":
                data = self._get_field(db, user, field_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "PUT":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                updated = self._update_field(db, user, field_id, payload, request_id)
                return self._success(updated, request_id=request_id, user_id=user.user_id)
            if method == "DELETE":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                deleted = self._soft_delete_field(db, user, field_id, request_id)
                return self._success(deleted, request_id=request_id, user_id=user.user_id)

        field_restore_id = self._match_id(path, r"^/api/v1/fields/(\d+)/restore$")
        if field_restore_id is not None and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            restored = self._restore_field(db, user, field_restore_id, request_id)
            return self._success(restored, request_id=request_id, user_id=user.user_id)

        field_history_id = self._match_id(path, r"^/api/v1/fields/(\d+)/history$")
        if field_history_id is not None and method == "GET":
            data = self._field_history(db, user, field_history_id, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_operations_id = self._match_id(path, r"^/api/v1/fields/(\d+)/operations$")
        if field_operations_id is not None:
            if method == "GET":
                data = self._list_field_operations(db, user, field_operations_id, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                created = self._create_field_operation(db, user, field_operations_id, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        # Crops
        if path == "/api/v1/crops":
            if method == "GET":
                data = self._list_crops(db, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                payload = self._parse_json_body(raw_body)
                created = self._create_crop(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        crop_id = self._match_id(path, r"^/api/v1/crops/(\d+)$")
        if crop_id is not None:
            if method == "GET":
                data = self._get_crop(db, crop_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "PUT":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
                payload = self._parse_json_body(raw_body)
                updated = self._update_crop(db, user, crop_id, payload, request_id)
                return self._success(updated, request_id=request_id, user_id=user.user_id)

        # Seasons
        if path == "/api/v1/seasons":
            if method == "GET":
                data = self._list_seasons(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                created = self._create_season(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        season_id = self._match_id(path, r"^/api/v1/seasons/(\d+)$")
        if season_id is not None:
            if method == "GET":
                data = self._get_season(db, user, season_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "PUT":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                updated = self._update_season(db, user, season_id, payload, request_id)
                return self._success(updated, request_id=request_id, user_id=user.user_id)

        # Data endpoints: weather/satellite/sync
        weather_field_id = self._match_id(path, r"^/api/v1/fields/(\d+)/weather$")
        if weather_field_id is not None and method == "GET":
            data, no_data = self._get_weather_series(db, user, weather_field_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        weather_summary_field_id = self._match_id(path, r"^/api/v1/fields/(\d+)/weather/summary$")
        if weather_summary_field_id is not None and method == "GET":
            data, no_data = self._get_weather_summary(db, user, weather_summary_field_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        sat_index_field_id = self._match_id(path, r"^/api/v1/fields/(\d+)/satellite/index$")
        if sat_index_field_id is not None and method == "GET":
            data, no_data = self._get_satellite_index(db, user, sat_index_field_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        sat_scenes_field_id = self._match_id(path, r"^/api/v1/fields/(\d+)/satellite/scenes$")
        if sat_scenes_field_id is not None and method == "GET":
            data, no_data = self._get_satellite_scenes(db, user, sat_scenes_field_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        sat_quality_field_id = self._match_id(path, r"^/api/v1/fields/(\d+)/satellite/quality$")
        if sat_quality_field_id is not None and method == "GET":
            data, no_data = self._get_satellite_quality(db, user, sat_quality_field_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        if path == "/api/v1/sync/status" and method == "GET":
            source = self._normalize_source(self._query_str(query, "source", required=True))
            data = get_sync_status(db, source)
            degradation = get_degradation_status(db, source)
            data["degradation_mode"] = degradation.get("degradation_mode")
            data["degradation_message"] = degradation.get("message")
            return self._success(data, request_id=request_id, user_id=user.user_id)

        if path == "/api/v1/sync/run" and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            payload = self._parse_json_body(raw_body)
            idempotency_key = self._header(headers, "idempotency-key")
            response = self._sync_run(db, user, payload, request_id, idempotency_key)
            return response

        # Map-first API (layers/grid/tiles/probe/zones)
        if path == "/api/v1/layers" and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._list_layers(db, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        layer_id_for_grid = self._match_text(path, r"^/api/v1/layers/([a-zA-Z0-9_.-]+)/grid$")
        if layer_id_for_grid is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data, no_data = self._get_layer_grid(db, user, layer_id_for_grid, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        layer_id_for_field = self._match_text(path, r"^/api/v1/layers/([a-zA-Z0-9_.-]+)/field$")
        if layer_id_for_field is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data, no_data = self._get_layer_field(db, user, layer_id_for_field, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        layer_tile_match = re.fullmatch(r"^/api/v1/layers/([a-zA-Z0-9_.-]+)/tiles/(\d+)/(\d+)/(\d+)$", path)
        if layer_tile_match is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            layer_id, z_raw, x_raw, y_raw = layer_tile_match.groups()
            return self._get_layer_tile(
                db,
                user,
                layer_id=layer_id,
                z=int(z_raw),
                x=int(x_raw),
                y=int(y_raw),
                query=query,
                request_headers=headers,
                request_id=request_id,
            )

        field_probe_id = self._match_id(path, r"^/api/v1/fields/(\d+)/probe$")
        if field_probe_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data, no_data = self._probe_field_layers(db, user, field_probe_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        field_zones_id = self._match_id(path, r"^/api/v1/fields/(\d+)/zones$")
        if field_zones_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_field_zones(db, user, field_zones_id, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_zonal_stats_id = self._match_id(path, r"^/api/v1/fields/(\d+)/zonal-stats$")
        if field_zonal_stats_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data, no_data = self._get_field_zonal_stats(db, user, field_zonal_stats_id, query)
            return self._success(
                data,
                request_id=request_id,
                user_id=user.user_id,
                meta_extra=self._no_data_meta(no_data, "Нет данных за выбранный период"),
                error_code="NO_DATA" if no_data else None,
            )

        field_algo_id = self._match_id(path, r"^/api/v1/fields/(\d+)/algorithms/gdd$")
        if field_algo_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_algorithm_gdd(db, user, field_algo_id, query, request_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_algo_id = self._match_id(path, r"^/api/v1/fields/(\d+)/algorithms/vpd$")
        if field_algo_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_algorithm_vpd(db, user, field_algo_id, query, request_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_algo_id = self._match_id(path, r"^/api/v1/fields/(\d+)/algorithms/et0$")
        if field_algo_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_algorithm_et0(db, user, field_algo_id, query, request_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_algo_id = self._match_id(path, r"^/api/v1/fields/(\d+)/algorithms/water-deficit$")
        if field_algo_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_algorithm_water_deficit(db, user, field_algo_id, query, request_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        if path == "/api/v1/stream" and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            return self._stream_events(db, user, request_id)

        # Assistant
        if path == "/api/v1/assistant/rules":
            if method == "GET":
                data = self._list_assistant_rules(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                created = self._create_assistant_rule(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        assistant_rule_id = self._match_id(path, r"^/api/v1/assistant/rules/(\d+)$")
        if assistant_rule_id is not None:
            if method == "PUT":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                updated = self._update_assistant_rule(db, user, assistant_rule_id, payload, request_id)
                return self._success(updated, request_id=request_id, user_id=user.user_id)
            if method == "DELETE":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                archived = self._archive_assistant_rule(db, user, assistant_rule_id, request_id)
                return self._success(archived, request_id=request_id, user_id=user.user_id)

        field_alerts_id = self._match_id(path, r"^/api/v1/fields/(\d+)/assistant/alerts$")
        if field_alerts_id is not None and method == "GET":
            data = self._get_assistant_alerts(db, user, field_alerts_id, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        field_recommendations_id = self._match_id(path, r"^/api/v1/fields/(\d+)/assistant/recommendations$")
        if field_recommendations_id is not None and method == "GET":
            data = self._get_assistant_recommendations(db, user, field_recommendations_id, query)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        if path == "/api/v1/assistant/decisions":
            if method == "GET":
                data = self._list_assistant_decisions(db, user, query)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "POST":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                created = self._create_assistant_decision(db, user, payload, request_id)
                return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        # Export
        if path == "/api/v1/export" and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            payload = self._parse_json_body(raw_body)
            idempotency_key = self._header(headers, "idempotency-key")
            response = self._create_export_job(db, user, payload, request_id, idempotency_key)
            return response

        export_id = self._match_text(path, r"^/api/v1/export/([a-zA-Z0-9_-]+)$")
        if export_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_export_job(db, user, export_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        export_extend_id = self._match_text(path, r"^/api/v1/export/([a-zA-Z0-9_-]+)/extend$")
        if export_extend_id is not None and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER})
            payload = self._parse_json_body(raw_body)
            days = self._int_value(payload.get("days"), "days", min_value=1, max_value=365)
            data = self._extend_export_job(db, user, export_extend_id, days, request_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        export_download_id = self._match_text(path, r"^/api/v1/export/([a-zA-Z0-9_-]+)/download$")
        if export_download_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            return self._download_export_job(db, user, export_download_id, request_id)

        # Scenario modeling
        if path == "/api/v1/modeling/scenarios" and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
            payload = self._parse_json_body(raw_body)
            created = self._create_scenario(db, user, payload, request_id)
            return self._success(created, request_id=request_id, status=201, user_id=user.user_id)

        scenario_id = self._match_text(path, r"^/api/v1/modeling/scenarios/([a-zA-Z0-9_-]+)$")
        if scenario_id is not None:
            if method == "GET":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
                data = self._get_scenario(db, user, scenario_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)
            if method == "PATCH":
                self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
                payload = self._parse_json_body(raw_body)
                data = self._update_scenario(db, user, scenario_id, payload, request_id)
                return self._success(data, request_id=request_id, user_id=user.user_id)

        scenario_run_id = self._match_text(path, r"^/api/v1/modeling/scenarios/([a-zA-Z0-9_-]+)/run$")
        if scenario_run_id is not None and method == "POST":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST})
            data = self._run_scenario(db, user, scenario_run_id, request_id)
            return self._success(data, request_id=request_id, status=202, user_id=user.user_id)

        scenario_result_id = self._match_text(path, r"^/api/v1/modeling/scenarios/([a-zA-Z0-9_-]+)/result$")
        if scenario_result_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_scenario_result(db, user, scenario_result_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        scenario_diff_id = self._match_text(path, r"^/api/v1/modeling/scenarios/([a-zA-Z0-9_-]+)/diff$")
        if scenario_diff_id is not None and method == "GET":
            self._require_roles(user, {ROLE_ADMIN, ROLE_MANAGER, ROLE_AGRONOMIST, ROLE_VIEWER})
            data = self._get_scenario_diff(db, user, scenario_diff_id)
            return self._success(data, request_id=request_id, user_id=user.user_id)

        raise ApiError("NOT_FOUND", "Объект не найден", status=404)

    # -------------------------------
    # Observability
    # -------------------------------
    def record_request(
        self,
        *,
        request_id: str,
        user_id: int | None,
        method: str,
        endpoint: str,
        status_code: int,
        duration_ms: int,
        error_code: str | None,
    ) -> None:
        event = {
            "request_id": request_id,
            "user_id": user_id,
            "endpoint": endpoint,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "error_code": error_code,
            "timestamp": _iso_utc(datetime.now(timezone.utc)),
        }
        print(json.dumps(event, ensure_ascii=False), flush=True)

        try:
            db = DbClient()
            db.ensure_ready()
            db.exec_checked(
                f"""
                INSERT INTO api_request_log (
                    request_id,
                    user_id,
                    endpoint,
                    method,
                    status_code,
                    duration_ms,
                    error_code
                ) VALUES (
                    {_sql_quote(request_id)},
                    {str(user_id) if user_id is not None else 'NULL'},
                    {_sql_quote(endpoint[:500])},
                    {_sql_quote(method)},
                    {int(status_code)},
                    {int(max(duration_ms, 0))},
                    {(_sql_quote(error_code) if error_code else 'NULL')}
                );
                """
            )
        except Exception:
            return

    # -------------------------------
    # Seed / auth / RBAC
    # -------------------------------
    def _ensure_stage5_seed(self, db: DbClient) -> None:
        if self._seed_done:
            return

        with self._seed_lock:
            if self._seed_done:
                return

            db.exec_checked(
                """
                INSERT INTO roles (code, name)
                VALUES
                    ('admin', 'Администратор'),
                    ('manager', 'Менеджер'),
                    ('agronomist', 'Агроном'),
                    ('viewer', 'Наблюдатель')
                ON CONFLICT (code) DO NOTHING;

                INSERT INTO enterprises (name)
                SELECT 'ООО Демонстрационное хозяйство API v1'
                WHERE NOT EXISTS (SELECT 1 FROM enterprises);
                """
            )

            db.exec_checked(
                """
                INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
                SELECT
                    (SELECT id FROM enterprises ORDER BY id LIMIT 1),
                    (SELECT id FROM roles WHERE code = 'admin'),
                    'admin@zemledar.local',
                    'Администратор API',
                    'hash',
                    TRUE
                WHERE NOT EXISTS (SELECT 1 FROM app_users WHERE email = 'admin@zemledar.local');

                INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
                SELECT
                    (SELECT id FROM enterprises ORDER BY id LIMIT 1),
                    (SELECT id FROM roles WHERE code = 'manager'),
                    'manager@zemledar.local',
                    'Менеджер API',
                    'hash',
                    TRUE
                WHERE NOT EXISTS (SELECT 1 FROM app_users WHERE email = 'manager@zemledar.local');

                INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
                SELECT
                    (SELECT id FROM enterprises ORDER BY id LIMIT 1),
                    (SELECT id FROM roles WHERE code = 'agronomist'),
                    'agronomist@zemledar.local',
                    'Агроном API',
                    'hash',
                    TRUE
                WHERE NOT EXISTS (SELECT 1 FROM app_users WHERE email = 'agronomist@zemledar.local');

                INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
                SELECT
                    (SELECT id FROM enterprises ORDER BY id LIMIT 1),
                    (SELECT id FROM roles WHERE code = 'viewer'),
                    'viewer@zemledar.local',
                    'Наблюдатель API',
                    'hash',
                    TRUE
                WHERE NOT EXISTS (SELECT 1 FROM app_users WHERE email = 'viewer@zemledar.local');
                """
            )

            db.exec_checked(
                """
                UPDATE enterprises e
                SET owner_user_id = (
                    SELECT id FROM app_users WHERE email = 'admin@zemledar.local' LIMIT 1
                )
                WHERE e.owner_user_id IS NULL;

                INSERT INTO crops (name)
                VALUES ('Пшеница')
                ON CONFLICT (name) DO NOTHING;
                """
            )

            self._seed_done = True

    def _resolve_user(self, db: DbClient, headers: dict[str, str]) -> UserContext:
        email = self._header(headers, "x-user-email") or "viewer@zemledar.local"
        payload = db.query_json(
            f"""
            SELECT row_to_json(u)
            FROM (
                SELECT
                    au.id AS user_id,
                    au.enterprise_id,
                    r.code AS role_code,
                    au.email,
                    au.full_name
                FROM app_users au
                JOIN roles r ON r.id = au.role_id
                WHERE au.email = {_sql_quote(email)}
                  AND au.is_active = TRUE
                ORDER BY au.id
                LIMIT 1
            ) u;
            """
        )

        if not isinstance(payload, dict):
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403, details="Пользователь не найден")

        role_code = str(payload.get("role_code") or "")
        normalized_role = ROLE_ALIASES.get(role_code)
        if normalized_role is None:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403, details="Неизвестная роль")

        return UserContext(
            user_id=int(payload["user_id"]),
            enterprise_id=(int(payload["enterprise_id"]) if payload.get("enterprise_id") is not None else None),
            role_code=normalized_role,
            email=str(payload["email"]),
            full_name=str(payload["full_name"]),
        )

    def _require_roles(self, user: UserContext, allowed: set[str]) -> None:
        if user.role_code not in allowed:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)

    # -------------------------------
    # Enterprise + users
    # -------------------------------
    def _list_enterprises(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        filter_name = self._query_str(query, "filter", required=False)

        where_parts = ["1=1"]
        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"e.id = {user.enterprise_id}")
        if filter_name:
            where_parts.append(f"e.name ILIKE {_sql_quote('%' + filter_name + '%')}")

        where_sql = " AND ".join(where_parts)
        sort_sql = self._sort_clause(self._query_str(query, "sort", required=False), {"id", "name", "created_at"}, "id")

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    e.id,
                    e.name,
                    e.owner_user_id,
                    to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM enterprises e
                WHERE {where_sql}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_enterprise(self, db: DbClient, user: UserContext, payload: dict[str, Any], request_id: str) -> dict[str, Any]:
        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)

        exists = db.exec_checked(
            f"""
            SELECT COUNT(*)
            FROM enterprises
            WHERE lower(name) = lower({_sql_quote(name)})
              AND COALESCE(owner_user_id, 0) = {user.user_id};
            """,
            tuples_only=True,
        )
        if self._scalar_int(exists) > 0:
            raise ApiError(
                "CONFLICT",
                "Конфликт состояния (повторная операция)",
                status=409,
                details="Предприятие с таким названием уже существует для владельца",
            )

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO enterprises (name, owner_user_id)
                VALUES ({_sql_quote(name)}, {user.user_id})
                RETURNING id, name, owner_user_id, created_at
            )
            SELECT row_to_json(x)
            FROM (
                SELECT
                    id,
                    name,
                    owner_user_id,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM ins
            ) x;
            """
        )
        assert isinstance(created, dict)
        self._write_audit(db, user.user_id, "enterprise.create", "enterprise", str(created["id"]), None, created, request_id)
        return created

    def _get_enterprise(self, db: DbClient, user: UserContext, enterprise_id: int) -> dict[str, Any]:
        if user.role_code != ROLE_ADMIN and user.enterprise_id != enterprise_id:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)

        payload = db.query_json(
            f"""
            SELECT row_to_json(e)
            FROM (
                SELECT
                    id,
                    name,
                    owner_user_id,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM enterprises
                WHERE id = {enterprise_id}
            ) e;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        return payload

    def _update_enterprise(
        self,
        db: DbClient,
        user: UserContext,
        enterprise_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        before = self._get_enterprise(db, user, enterprise_id)
        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)

        db.exec_checked(
            f"""
            UPDATE enterprises
            SET name = {_sql_quote(name)}
            WHERE id = {enterprise_id};
            """
        )
        after = self._get_enterprise(db, user, enterprise_id)
        self._write_audit(db, user.user_id, "enterprise.update", "enterprise", str(enterprise_id), before, after, request_id)
        return after

    def _bind_user_to_enterprise(
        self,
        db: DbClient,
        user: UserContext,
        enterprise_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        user_email = self._str_value(payload.get("user_email"), "user_email", min_len=5, max_len=250)
        before = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT id, enterprise_id, email FROM app_users WHERE email = {_sql_quote(user_email)}
            ) x;
            """
        )
        if not isinstance(before, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404, details="Пользователь не найден")

        db.exec_checked(
            f"""
            UPDATE app_users
            SET enterprise_id = {enterprise_id}
            WHERE email = {_sql_quote(user_email)};
            """
        )

        after = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT id, enterprise_id, email FROM app_users WHERE email = {_sql_quote(user_email)}
            ) x;
            """
        )
        assert isinstance(after, dict)
        self._write_audit(
            db,
            user.user_id,
            "enterprise.bind_user",
            "user",
            str(after.get("id")),
            before,
            after,
            request_id,
        )
        return after

    def _list_users(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        where_parts = ["u.is_active = TRUE"]

        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"u.enterprise_id = {user.enterprise_id}")

        role_filter = self._query_str(query, "role", required=False)
        if role_filter:
            normalized_role = ROLE_ALIASES.get(role_filter.lower())
            if not normalized_role:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: неизвестная роль", status=422)
            where_parts.append(f"r.code = {_sql_quote(normalized_role)}")

        where_sql = " AND ".join(where_parts)
        sort_sql = self._sort_clause(self._query_str(query, "sort", required=False), {"id", "email", "created_at"}, "id")

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    u.id,
                    u.enterprise_id,
                    r.code AS role,
                    u.email,
                    u.full_name,
                    to_char(u.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM app_users u
                JOIN roles r ON r.id = u.role_id
                WHERE {where_sql}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_user(
        self,
        db: DbClient,
        actor: UserContext,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        email = self._str_value(payload.get("email"), "email", min_len=5, max_len=250)
        full_name = self._str_value(payload.get("full_name"), "full_name", min_len=2, max_len=250)
        role_raw = self._str_value(payload.get("role"), "role", min_len=3, max_len=30)
        role_code = ROLE_ALIASES.get(role_raw.lower())
        if role_code is None:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: неизвестная роль", status=422)

        enterprise_id = self._int_value(payload.get("enterprise_id"), "enterprise_id", min_value=1)
        if actor.role_code != ROLE_ADMIN and actor.enterprise_id != enterprise_id:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO app_users (enterprise_id, role_id, email, full_name, password_hash, is_active)
                VALUES (
                    {enterprise_id},
                    (SELECT id FROM roles WHERE code = {_sql_quote(role_code)} LIMIT 1),
                    {_sql_quote(email)},
                    {_sql_quote(full_name)},
                    'hash',
                    TRUE
                )
                RETURNING id, enterprise_id, role_id, email, full_name, created_at
            )
            SELECT row_to_json(x)
            FROM (
                SELECT
                    i.id,
                    i.enterprise_id,
                    r.code AS role,
                    i.email,
                    i.full_name,
                    to_char(i.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM ins i
                JOIN roles r ON r.id = i.role_id
            ) x;
            """
        )

        if not isinstance(created, dict):
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409)

        self._write_audit(db, actor.user_id, "user.create", "user", str(created["id"]), None, created, request_id)
        return created

    # -------------------------------
    # Fields + history + operations
    # -------------------------------
    def _list_fields(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        include_deleted = self._query_bool(query, "with_deleted", default=False)

        where_parts = ["1=1"]
        if not include_deleted:
            where_parts.append("f.deleted_at IS NULL")

        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"f.enterprise_id = {user.enterprise_id}")

        enterprise_filter = self._query_int(query, "enterprise_id")
        if enterprise_filter is not None:
            if user.role_code != ROLE_ADMIN and user.enterprise_id != enterprise_filter:
                raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)
            where_parts.append(f"f.enterprise_id = {enterprise_filter}")

        filter_text = self._query_str(query, "filter", required=False)
        if filter_text:
            where_parts.append(f"f.name ILIKE {_sql_quote('%' + filter_text + '%')}")

        where_sql = " AND ".join(where_parts)
        sort_sql = self._sort_clause(
            self._query_str(query, "sort", required=False),
            {"id", "name", "created_at", "updated_at", "area_ha"},
            "id",
        )

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    f.id,
                    f.enterprise_id,
                    f.season_id,
                    f.name,
                    ROUND(f.area_ha::numeric, 4) AS area_ha,
                    ST_AsGeoJSON(f.geom)::json AS geometry,
                    ST_AsGeoJSON(f.bbox)::json AS bbox,
                    to_char(f.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(f.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
                    CASE
                        WHEN f.deleted_at IS NULL THEN NULL
                        ELSE to_char(f.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS deleted_at
                FROM fields f
                WHERE {where_sql}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _get_field(self, db: DbClient, user: UserContext, field_id: int, *, include_deleted: bool = True) -> dict[str, Any]:
        where_parts = [f"f.id = {field_id}"]
        if not include_deleted:
            where_parts.append("f.deleted_at IS NULL")
        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"f.enterprise_id = {user.enterprise_id}")

        where_sql = " AND ".join(where_parts)
        payload = db.query_json(
            f"""
            SELECT row_to_json(fx)
            FROM (
                SELECT
                    f.id,
                    f.enterprise_id,
                    f.season_id,
                    f.name,
                    ROUND(f.area_ha::numeric, 4) AS area_ha,
                    ST_AsGeoJSON(f.geom)::json AS geometry,
                    ST_AsGeoJSON(f.bbox)::json AS bbox,
                    to_char(f.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(f.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
                    CASE
                        WHEN f.deleted_at IS NULL THEN NULL
                        ELSE to_char(f.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS deleted_at
                FROM fields f
                WHERE {where_sql}
            ) fx;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        return payload

    def _create_field(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        enterprise_id = self._int_value(payload.get("enterprise_id"), "enterprise_id", min_value=1)
        if user.role_code != ROLE_ADMIN and user.enterprise_id != enterprise_id:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)

        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)
        geom_sql = self._geometry_sql(payload)

        try:
            created_id_raw = db.exec_checked(
                f"""
                INSERT INTO fields (enterprise_id, season_id, name, geom)
                VALUES ({enterprise_id}, NULL, {_sql_quote(name)}, {geom_sql})
                RETURNING id;
                """,
                tuples_only=True,
            )
        except Stage3Error as exc:
            raise self._map_geometry_error(exc)

        field_id = self._scalar_int(created_id_raw)
        after = self._get_field(db, user, field_id)
        self._write_audit(db, user.user_id, "field.create", "field", str(field_id), None, after, request_id)
        return after

    def _update_field(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        before = self._get_field(db, user, field_id)

        name = payload.get("name")
        geom_update = payload.get("geojson") is not None or payload.get("wkt") is not None or payload.get("geometry") is not None
        set_parts: list[str] = []

        if name is not None:
            set_parts.append(f"name = {_sql_quote(self._str_value(name, 'name', min_len=2, max_len=250))}")

        if geom_update:
            geom_sql = self._geometry_sql(payload)
            set_parts.append(f"geom = {geom_sql}")

        if not set_parts:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: нечего обновлять", status=422)

        try:
            db.exec_checked(
                f"""
                UPDATE fields
                SET {', '.join(set_parts)}
                WHERE id = {field_id};
                """
            )
        except Stage3Error as exc:
            raise self._map_geometry_error(exc)

        after = self._get_field(db, user, field_id)

        if geom_update:
            old_geom_json = json.dumps(before.get("geometry"), ensure_ascii=False)
            new_geom_json = json.dumps(after.get("geometry"), ensure_ascii=False)
            db.exec_checked(
                f"""
                INSERT INTO field_geometry_history (field_id, changed_by, request_id, old_geom, new_geom)
                VALUES (
                    {field_id},
                    {user.user_id},
                    {_sql_quote(request_id)},
                    ST_SetSRID(ST_GeomFromGeoJSON($${old_geom_json}$$), 4326),
                    ST_SetSRID(ST_GeomFromGeoJSON($${new_geom_json}$$), 4326)
                );
                """
            )

        self._write_audit(db, user.user_id, "field.update", "field", str(field_id), before, after, request_id)
        return after

    def _soft_delete_field(self, db: DbClient, user: UserContext, field_id: int, request_id: str) -> dict[str, Any]:
        before = self._get_field(db, user, field_id)
        db.exec_checked(
            f"""
            UPDATE fields
            SET deleted_at = NOW()
            WHERE id = {field_id};
            """
        )
        after = self._get_field(db, user, field_id)
        self._write_audit(db, user.user_id, "field.soft_delete", "field", str(field_id), before, after, request_id)
        return after

    def _restore_field(self, db: DbClient, user: UserContext, field_id: int, request_id: str) -> dict[str, Any]:
        before = self._get_field(db, user, field_id)
        db.exec_checked(
            f"""
            UPDATE fields
            SET deleted_at = NULL
            WHERE id = {field_id};
            """
        )
        after = self._get_field(db, user, field_id)
        self._write_audit(db, user.user_id, "field.restore", "field", str(field_id), before, after, request_id)
        return after

    def _field_history(self, db: DbClient, user: UserContext, field_id: int, query: dict[str, list[str]]) -> dict[str, Any]:
        _ = self._get_field(db, user, field_id)
        page, page_size, offset = self._pagination(query)

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    h.id,
                    h.field_id,
                    h.changed_by,
                    h.request_id,
                    ST_AsGeoJSON(h.old_geom)::json AS old_geometry,
                    ST_AsGeoJSON(h.new_geom)::json AS new_geometry,
                    to_char(h.changed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS changed_at
                FROM field_geometry_history h
                WHERE h.field_id = {field_id}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY changed_at DESC
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_field_operation(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        operation_type = self._str_value(payload.get("operation_type"), "operation_type", min_len=3, max_len=30)
        operation_at = self._iso_required(payload.get("operation_at"), "operation_at")
        comment = payload.get("comment")
        comment_sql = _sql_quote(self._str_value(comment, "comment", min_len=0, max_len=2000)) if comment else "NULL"

        point_sql = self._optional_point_sql(payload)
        zone_sql = self._optional_zone_sql(payload)

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO field_operations (
                    field_id,
                    user_id,
                    operation_type,
                    operation_at,
                    comment,
                    point_geom,
                    zone_geom
                ) VALUES (
                    {field_id},
                    {user.user_id},
                    {_sql_quote(operation_type)},
                    {_sql_quote(operation_at)}::timestamptz,
                    {comment_sql},
                    {point_sql},
                    {zone_sql}
                )
                RETURNING id, field_id, user_id, operation_type, operation_at, comment, point_geom, zone_geom, created_at
            )
            SELECT row_to_json(x)
            FROM (
                SELECT
                    id,
                    field_id,
                    user_id,
                    operation_type,
                    to_char(operation_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS operation_at,
                    comment,
                    ST_AsGeoJSON(point_geom)::json AS point_geometry,
                    ST_AsGeoJSON(zone_geom)::json AS zone_geometry,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM ins
            ) x;
            """
        )
        assert isinstance(created, dict)

        self._write_audit(db, user.user_id, "operation.create", "field_operation", str(created["id"]), None, created, request_id)
        return created

    def _list_field_operations(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        page, page_size, offset = self._pagination(query)
        sort_sql = self._sort_clause(
            self._query_str(query, "sort", required=False),
            {"id", "operation_at", "created_at"},
            "operation_at",
        )

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    fo.id,
                    fo.field_id,
                    fo.user_id,
                    fo.operation_type,
                    to_char(fo.operation_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS operation_at,
                    fo.comment,
                    ST_AsGeoJSON(fo.point_geom)::json AS point_geometry,
                    ST_AsGeoJSON(fo.zone_geom)::json AS zone_geometry,
                    to_char(fo.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM field_operations fo
                WHERE fo.field_id = {field_id}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    # -------------------------------
    # Crops + seasons
    # -------------------------------
    def _list_crops(self, db: DbClient, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        filter_text = self._query_str(query, "filter", required=False)
        where_sql = "1=1"
        if filter_text:
            where_sql = f"name ILIKE {_sql_quote('%' + filter_text + '%')}"

        sort_sql = self._sort_clause(self._query_str(query, "sort", required=False), {"id", "name", "created_at"}, "id")

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    id,
                    name,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM crops
                WHERE {where_sql}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_crop(self, db: DbClient, user: UserContext, payload: dict[str, Any], request_id: str) -> dict[str, Any]:
        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)
        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO crops (name)
                VALUES ({_sql_quote(name)})
                RETURNING id, name, created_at
            )
            SELECT row_to_json(x)
            FROM (
                SELECT id, name, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM ins
            ) x;
            """
        )
        if not isinstance(created, dict):
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409)

        self._write_audit(db, user.user_id, "crop.create", "crop", str(created["id"]), None, created, request_id)
        return created

    def _get_crop(self, db: DbClient, crop_id: int) -> dict[str, Any]:
        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    id,
                    name,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM crops
                WHERE id = {crop_id}
            ) x;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        return payload

    def _update_crop(self, db: DbClient, user: UserContext, crop_id: int, payload: dict[str, Any], request_id: str) -> dict[str, Any]:
        before = self._get_crop(db, crop_id)
        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)
        db.exec_checked(
            f"""
            UPDATE crops
            SET name = {_sql_quote(name)}
            WHERE id = {crop_id};
            """
        )
        after = self._get_crop(db, crop_id)
        self._write_audit(db, user.user_id, "crop.update", "crop", str(crop_id), before, after, request_id)
        return after

    def _list_seasons(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        where_parts = ["1=1"]

        field_id = self._query_int(query, "field_id")
        if field_id is not None:
            where_parts.append(f"s.field_id = {field_id}")

        status_filter = self._query_str(query, "status", required=False)
        if status_filter:
            if status_filter not in {"active", "archived"}:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: status", status=422)
            where_parts.append(f"s.status = {_sql_quote(status_filter)}")

        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"s.enterprise_id = {user.enterprise_id}")

        where_sql = " AND ".join(where_parts)
        sort_sql = self._sort_clause(
            self._query_str(query, "sort", required=False),
            {"id", "started_at", "ended_at", "created_at"},
            "started_at",
        )

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    s.id,
                    s.enterprise_id,
                    s.field_id,
                    s.crop_id,
                    c.name AS crop_name,
                    s.year,
                    s.name,
                    s.status,
                    s.started_at,
                    s.ended_at,
                    s.close_reason,
                    to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM seasons s
                JOIN crops c ON c.id = s.crop_id
                WHERE {where_sql}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_season(self, db: DbClient, user: UserContext, payload: dict[str, Any], request_id: str) -> dict[str, Any]:
        field_id = self._int_value(payload.get("field_id"), "field_id", min_value=1)
        field = self._get_field(db, user, field_id, include_deleted=False)
        enterprise_id = int(field["enterprise_id"])
        self._assert_enterprise_scope(user, enterprise_id)

        crop_id = self._int_value(payload.get("crop_id"), "crop_id", min_value=1)
        name = self._str_value(payload.get("name"), "name", min_len=2, max_len=250)
        year = self._int_value(payload.get("year"), "year", min_value=2000, max_value=2100)
        started_at = self._date_value(payload.get("started_at"), "started_at")
        ended_at = self._date_optional(payload.get("ended_at"), "ended_at")
        status = self._str_value(payload.get("status", "active"), "status", min_len=6, max_len=10)
        if status not in {"active", "archived"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: status", status=422)

        close_reason = payload.get("close_reason")
        close_reason_sql = _sql_quote(self._str_value(close_reason, "close_reason", min_len=2, max_len=1000)) if close_reason else "NULL"

        if status == "archived" and close_reason_sql == "NULL":
            raise ApiError(
                "VALIDATION_ERROR",
                "Некорректные входные данные: сезон нельзя закрыть без обязательных данных",
                status=422,
            )

        self._ensure_no_active_season_overlap(db, field_id, started_at, ended_at, exclude_id=None, status=status)

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO seasons (
                    enterprise_id,
                    field_id,
                    crop_id,
                    year,
                    name,
                    started_at,
                    ended_at,
                    status,
                    close_reason
                ) VALUES (
                    {enterprise_id},
                    {field_id},
                    {crop_id},
                    {year},
                    {_sql_quote(name)},
                    {_sql_quote(started_at)}::date,
                    {(_sql_quote(ended_at) + '::date') if ended_at else 'NULL'},
                    {_sql_quote(status)},
                    {close_reason_sql}
                )
                RETURNING
                    id,
                    enterprise_id,
                    field_id,
                    crop_id,
                    year,
                    name,
                    status,
                    started_at,
                    ended_at,
                    close_reason,
                    created_at
            )
            SELECT row_to_json(x)
            FROM (
                SELECT
                    i.id,
                    i.enterprise_id,
                    i.field_id,
                    i.crop_id,
                    c.name AS crop_name,
                    i.year,
                    i.name,
                    i.status,
                    i.started_at,
                    i.ended_at,
                    i.close_reason,
                    to_char(i.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM ins i
                JOIN crops c ON c.id = i.crop_id
            ) x;
            """
        )
        assert isinstance(created, dict)
        self._write_audit(db, user.user_id, "season.create", "season", str(created["id"]), None, created, request_id)
        return created

    def _get_season(self, db: DbClient, user: UserContext, season_id: int) -> dict[str, Any]:
        where_parts = [f"s.id = {season_id}"]
        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"s.enterprise_id = {user.enterprise_id}")

        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    s.id,
                    s.enterprise_id,
                    s.field_id,
                    s.crop_id,
                    c.name AS crop_name,
                    s.year,
                    s.name,
                    s.status,
                    s.started_at,
                    s.ended_at,
                    s.close_reason,
                    to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM seasons s
                JOIN crops c ON c.id = s.crop_id
                WHERE {' AND '.join(where_parts)}
            ) x;
            """
        )

        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        return payload

    def _update_season(
        self,
        db: DbClient,
        user: UserContext,
        season_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        before = self._get_season(db, user, season_id)

        field_id = int(before.get("field_id") or 0)
        if field_id <= 0:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: season не привязан к полю", status=422)

        started_at = self._date_optional(payload.get("started_at"), "started_at") or str(before.get("started_at"))
        ended_at = self._date_optional(payload.get("ended_at"), "ended_at")
        if ended_at is None and before.get("ended_at"):
            ended_at = str(before.get("ended_at"))

        status = payload.get("status", before.get("status"))
        status = self._str_value(status, "status", min_len=6, max_len=10)
        if status not in {"active", "archived"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: status", status=422)

        close_reason_input = payload.get("close_reason", before.get("close_reason"))
        close_reason_sql = (
            _sql_quote(self._str_value(close_reason_input, "close_reason", min_len=2, max_len=1000))
            if close_reason_input
            else "NULL"
        )

        if status == "archived" and close_reason_sql == "NULL":
            raise ApiError(
                "VALIDATION_ERROR",
                "Некорректные входные данные: сезон нельзя закрыть без обязательных данных",
                status=422,
            )

        self._ensure_no_active_season_overlap(db, field_id, started_at, ended_at, exclude_id=season_id, status=status)

        name = payload.get("name", before.get("name"))
        name = self._str_value(name, "name", min_len=2, max_len=250)

        db.exec_checked(
            f"""
            UPDATE seasons
            SET
                name = {_sql_quote(name)},
                started_at = {_sql_quote(started_at)}::date,
                ended_at = {(_sql_quote(ended_at) + '::date') if ended_at else 'NULL'},
                status = {_sql_quote(status)},
                close_reason = {close_reason_sql}
            WHERE id = {season_id};
            """
        )

        after = self._get_season(db, user, season_id)
        self._write_audit(db, user.user_id, "season.update", "season", str(season_id), before, after, request_id)
        return after

    def _ensure_no_active_season_overlap(
        self,
        db: DbClient,
        field_id: int,
        started_at: str,
        ended_at: str | None,
        *,
        exclude_id: int | None,
        status: str,
    ) -> None:
        if status != "active":
            return

        end_value = ended_at or "infinity"
        exclude_sql = f"AND id <> {exclude_id}" if exclude_id is not None else ""

        overlap_count = db.exec_checked(
            f"""
            SELECT COUNT(*)
            FROM seasons
            WHERE field_id = {field_id}
              AND status = 'active'
              {exclude_sql}
              AND daterange(started_at, COALESCE(ended_at, 'infinity'::date), '[]')
                  && daterange({_sql_quote(started_at)}::date, COALESCE({_sql_quote(end_value)}::date, 'infinity'::date), '[]');
            """,
            tuples_only=True,
        )

        if self._scalar_int(overlap_count) > 0:
            raise ApiError(
                "VALIDATION_ERROR",
                "Некорректные входные данные: пересечение активных сезонов на одном поле",
                status=422,
            )

    # -------------------------------
    # Data endpoints
    # -------------------------------
    def _get_weather_series(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        granularity = self._query_str(query, "granularity", required=False) or "day"
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        report = query_range(
            db,
            source=source,
            field_id=field_id,
            range_start=range_start,
            range_end=range_end,
            granularity=granularity,
        )

        records = [item for item in report.get("records", []) if item.get("metric") in WEATHER_METRICS]
        summary = [item for item in report.get("summary", []) if item.get("metric") in WEATHER_METRICS]
        bins = report.get("time_bins", [])

        coverage = self._coverage_percent(range_start, range_end, len(records), len(WEATHER_METRICS))
        last_sync = report.get("last_sync", {}).get("last_success_at") or report.get("last_sync", {}).get("last_sync_at")
        quality_flags_summary = self._quality_flags_summary(records)

        result = {
            "field_id": field_id,
            "source": source,
            "granularity": granularity,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "values": records,
            "summary": summary,
            "time_bins": bins,
            "meta": {
                "last_sync_at": last_sync,
                "data_coverage": coverage,
                "quality_flags": quality_flags_summary,
                "contract_version": CONTRACT_VERSION,
            },
        }
        return result, len(records) == 0

    def _get_weather_summary(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        series, no_data = self._get_weather_series(db, user, field_id, query)

        totals: dict[str, dict[str, Any]] = {}
        for record in series.get("values", []):
            metric = str(record.get("metric"))
            value = float(record.get("value") or 0.0)
            unit = str(record.get("unit") or "")
            bucket = totals.setdefault(metric, {"metric": metric, "unit": unit, "sum": 0.0, "count": 0, "min": value, "max": value})
            bucket["sum"] = float(bucket["sum"]) + value
            bucket["count"] = int(bucket["count"]) + 1
            bucket["min"] = min(float(bucket["min"]), value)
            bucket["max"] = max(float(bucket["max"]), value)

        items: list[dict[str, Any]] = []
        for metric in sorted(totals.keys()):
            bucket = totals[metric]
            count = max(1, int(bucket["count"]))
            items.append(
                {
                    "metric": metric,
                    "unit": bucket["unit"],
                    "min": round(float(bucket["min"]), 4),
                    "max": round(float(bucket["max"]), 4),
                    "mean": round(float(bucket["sum"]) / count, 4),
                    "sum": round(float(bucket["sum"]), 4),
                }
            )

        result = {
            "field_id": field_id,
            "source": series["source"],
            "from": series["from"],
            "to": series["to"],
            "aggregates": items,
            "meta": series["meta"],
        }
        return result, no_data

    def _get_satellite_index(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        index_type = self._str_value(self._query_str(query, "type", required=True), "type", min_len=4, max_len=5).lower()
        if index_type not in SATELLITE_METRICS:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: type", status=422)

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        report = query_range(
            db,
            source=source,
            field_id=field_id,
            range_start=range_start,
            range_end=range_end,
            granularity="hour",
        )

        records = [item for item in report.get("records", []) if item.get("metric") == index_type]

        for row in records:
            flags = [str(flag) for flag in row.get("quality_flags", [])]
            unreliable = any(flag in {"cloudy", "low_confidence", "cloud_mask_interpolated"} for flag in flags)
            row["quality_status"] = "недостоверно" if unreliable else "достоверно"
            row["quality_level"] = "LOW_QUALITY" if unreliable else "OK"

        last_sync = report.get("last_sync", {}).get("last_success_at") or report.get("last_sync", {}).get("last_sync_at")
        quality_flags_summary = self._quality_flags_summary(records)
        coverage = self._coverage_percent(range_start, range_end, len(records), 1)

        result = {
            "field_id": field_id,
            "source": source,
            "index_type": index_type,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "values": records,
            "meta": {
                "last_sync_at": last_sync,
                "data_coverage": coverage,
                "quality_flags": quality_flags_summary,
                "contract_version": CONTRACT_VERSION,
            },
        }
        return result, len(records) == 0

    def _get_satellite_scenes(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        payload = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.timestamp DESC), '[]'::json)
            FROM (
                SELECT
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    MAX(value) FILTER (WHERE metric_code = 'ndvi') AS ndvi,
                    MAX(value) FILTER (WHERE metric_code = 'ndre') AS ndre,
                    MAX(value) FILTER (WHERE metric_code = 'ndmi') AS ndmi,
                    MAX(value) FILTER (WHERE metric_code = 'cloudiness') AS cloudiness,
                    MAX(value) FILTER (WHERE metric_code = 'cloud_total') AS cloud_total,
                    MAX(value) FILTER (WHERE metric_code = 'cloud_mask') AS cloud_mask,
                    source,
                    CASE
                        WHEN COALESCE(MAX(value) FILTER (WHERE metric_code = 'cloud_mask'), 0) >= 60 THEN 'недостоверно'
                        WHEN COALESCE(MAX(value) FILTER (WHERE metric_code = 'cloud_total'), MAX(value) FILTER (WHERE metric_code = 'cloudiness'), 0) >= 70 THEN 'недостоверно'
                        ELSE 'достоверно'
                    END AS quality_status
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
                  AND metric_code IN ('ndvi', 'ndre', 'ndmi', 'cloudiness', 'cloud_total', 'cloud_mask')
                GROUP BY observed_at, source
            ) x;
            """
        )
        assert isinstance(payload, list)

        last_sync = get_sync_status(db, source).get("last_success_at")
        coverage = self._coverage_percent(range_start, range_end, len(payload), 1)
        result = {
            "field_id": field_id,
            "source": source,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "scenes": payload,
            "meta": {
                "last_sync_at": last_sync,
                "data_coverage": coverage,
                "contract_version": CONTRACT_VERSION,
            },
        }
        return result, len(payload) == 0

    def _get_satellite_quality(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        payload = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.timestamp DESC), '[]'::json)
            FROM (
                SELECT
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    metric_code AS metric,
                    ROUND(value::numeric, 4) AS value,
                    unit,
                    source,
                    quality_flags,
                    CASE
                        WHEN metric_code = 'cloud_mask' AND value >= 60 THEN 'недостоверно'
                        WHEN metric_code IN ('cloudiness', 'cloud_total') AND value >= 70 THEN 'недостоверно'
                        WHEN quality_flags::text ILIKE '%cloudy%' THEN 'недостоверно'
                        WHEN quality_flags::text ILIKE '%low_quality%' THEN 'недостоверно'
                        ELSE 'достоверно'
                    END AS quality_status,
                    CASE
                        WHEN metric_code = 'cloud_mask' AND value >= 60 THEN 'LOW_QUALITY'
                        WHEN metric_code IN ('cloudiness', 'cloud_total') AND value >= 70 THEN 'LOW_QUALITY'
                        WHEN quality_flags::text ILIKE '%cloudy%' THEN 'LOW_QUALITY'
                        WHEN quality_flags::text ILIKE '%low_quality%' THEN 'LOW_QUALITY'
                        ELSE 'OK'
                    END AS quality_level
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
                  AND metric_code IN ('cloudiness', 'cloud_total', 'cloud_mask')
            ) x;
            """
        )
        assert isinstance(payload, list)

        last_sync = get_sync_status(db, source).get("last_success_at")
        quality_flags_summary = self._quality_flags_summary(payload)
        coverage = self._coverage_percent(range_start, range_end, len(payload), len(SATELLITE_QUALITY_METRICS))
        result = {
            "field_id": field_id,
            "source": source,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "quality": payload,
            "meta": {
                "last_sync_at": last_sync,
                "data_coverage": coverage,
                "quality_flags": quality_flags_summary,
                "contract_version": CONTRACT_VERSION,
            },
        }
        return result, len(payload) == 0

    def _sync_run(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
        idempotency_key: str | None,
    ) -> ApiHttpResponse:
        source = self._normalize_source(self._str_value(payload.get("source"), "source", min_len=4, max_len=20))
        hours = self._int_value(payload.get("hours", 24), "hours", min_value=1, max_value=5000)
        retention_days = self._int_value(payload.get("retention_days", 30), "retention_days", min_value=1, max_value=365)
        field_id = self._int_optional(payload.get("field_id"), "field_id")

        endpoint = "/api/v1/sync/run"
        request_hash = self._request_hash(payload)
        if idempotency_key:
            replay = self._idempotency_lookup(db, idempotency_key, endpoint, request_hash)
            if replay is not None:
                return self._success(
                    replay["data"],
                    request_id=request_id,
                    status=int(replay["status_code"]),
                    user_id=user.user_id,
                    meta_extra={"idempotent_replay": True},
                )

        report = run_sync(db, source=source, hours=hours, field_id=field_id, retention_days=retention_days)
        self._write_audit(db, user.user_id, "sync.run", "provider", source, None, report, request_id)

        status_code = 202
        response_payload = {
            "data": report,
            "meta": {
                "api_version": API_VERSION,
                "request_id": request_id,
            },
        }

        if idempotency_key:
            self._idempotency_store(db, idempotency_key, endpoint, request_hash, response_payload, status_code)

        return ApiHttpResponse(
            status_code=status_code,
            body=self._json_bytes(response_payload),
            user_id=user.user_id,
        )

    # -------------------------------
    # Map-first API: layers/grid/tiles/probe/zones + stream
    # -------------------------------
    def _list_layers(self, db: DbClient, query: dict[str, list[str]]) -> dict[str, Any]:
        source_raw = self._query_str(query, "source", required=False)
        source = self._normalize_source(source_raw) if source_raw else None

        where_sql = "lr.is_active = TRUE"
        if source:
            where_sql += f" AND lr.source = {_sql_quote(source)}"

        rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.layer_id, x.source), '[]'::json)
            FROM (
                SELECT
                    lr.layer_id,
                    lr.title_ru,
                    lr.category,
                    lr.value_type,
                    lr.units,
                    lr.time_available,
                    lr.default_granularity,
                    lr.max_lookback_days,
                    lr.spatial_modes,
                    lr.zoom_rules,
                    lr.grid_sizes_m,
                    lr.legend,
                    lr.has_quality_flags,
                    lr.quality_rules,
                    lr.source,
                    lr.status AS registry_status,
                    CASE
                        WHEN ps.last_success_at IS NULL THEN to_char(ps.last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                        ELSE to_char(ps.last_success_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_sync_at,
                    ps.status AS sync_status
                FROM api_layer_registry lr
                LEFT JOIN provider_sync_status ps ON ps.source = lr.source
                WHERE {where_sql}
            ) x;
            """
        )
        assert isinstance(rows, list)

        items: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            sync_status = str(row.get("sync_status") or "")
            registry_status = str(row.get("registry_status") or "OK")
            source_status = self._source_status_label(sync_status, registry_status)
            items.append(
                {
                    "layer_id": row.get("layer_id"),
                    "title_ru": row.get("title_ru"),
                    "category": row.get("category"),
                    "value_type": row.get("value_type"),
                    "units": row.get("units"),
                    "time_support": {
                        "available": row.get("time_available") or [],
                        "default_granularity": row.get("default_granularity"),
                        "max_lookback_days": row.get("max_lookback_days"),
                    },
                    "spatial_support": {
                        "modes": row.get("spatial_modes") or [],
                        "zoom_levels": row.get("zoom_rules") or {},
                        "grid_sizes_m": row.get("grid_sizes_m") or [],
                    },
                    "legend": row.get("legend") or {},
                    "quality": {
                        "has_quality_flags": bool(row.get("has_quality_flags")),
                        "quality_rules": row.get("quality_rules"),
                    },
                    "source_meta": {
                        "source": str(row.get("source") or "").lower(),
                        "last_sync_at": row.get("last_sync_at"),
                        "status": source_status,
                    },
                }
            )

        return {"items": items, "total": len(items)}

    def _get_layer_grid(
        self,
        db: DbClient,
        user: UserContext,
        layer_id: str,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        layer = self._load_layer(db, layer_id, source)

        bbox = self._parse_bbox(self._query_str(query, "bbox", required=True))
        zoom = self._int_value(self._query_str(query, "zoom", required=True), "zoom", min_value=0, max_value=22)
        field_id = self._query_int(query, "field_id")
        if field_id is not None:
            field = self._get_field(db, user, field_id, include_deleted=False)
            self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        range_start, range_end = self._resolve_time_range(query)
        granularity = self._query_str(query, "granularity", required=False) or str(layer.get("default_granularity") or "hour")
        allowed_granularities = {str(item) for item in (layer.get("time_available") or [])}
        if granularity not in allowed_granularities:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: granularity", status=422)

        agg = self._query_str(query, "agg", required=False) or "mean"
        if agg not in SCALAR_AGGREGATIONS:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: agg", status=422)

        rows = self._load_layer_points(
            db,
            source=source,
            layer_id=layer_id,
            range_start=range_start,
            range_end=range_end,
            field_id=field_id,
            enterprise_id=(None if user.role_code == ROLE_ADMIN else user.enterprise_id),
        )
        no_data = len(rows) == 0

        spatial_mode, cell_size_m = self._select_spatial_mode(layer, zoom, prefer="grid")
        values_by_metric: dict[str, list[float]] = {}
        for row in rows:
            metric = str(row.get("metric") or "")
            value = row.get("value")
            if metric not in values_by_metric:
                values_by_metric[metric] = []
            if value is not None:
                values_by_metric[metric].append(float(value))

        aggregated_by_metric: dict[str, float] = {}
        for metric_name, metric_values in values_by_metric.items():
            if metric_values:
                aggregated_by_metric[metric_name] = self._aggregate_values(metric_values, agg)

        cells: list[dict[str, Any]] = []
        if not no_data:
            def build_payload(ix: int, iy: int, center_lon: float, center_lat: float) -> dict[str, Any]:
                jitter = ((ix * 92821 + iy * 68917 + len(layer_id)) % 100) / 1000.0
                if str(layer.get("value_type")) == "vector":
                    base_speed = float(aggregated_by_metric.get("wind_speed") or 0.0)
                    speed = max(0.0, base_speed * (0.95 + jitter))
                    direction = float((ix * 31 + iy * 17 + 180) % 360)
                    rad = math.radians(direction)
                    u = round(speed * math.sin(rad), 4)
                    v = round(speed * math.cos(rad), 4)
                    return {
                        "u": round(u, 4),
                        "v": round(v, 4),
                        "speed": round(speed, 4),
                        "direction_deg": round(direction, 2),
                        "units": layer.get("units"),
                    }

                value = self._layer_scalar_value(layer_id, aggregated_by_metric, ix, iy)
                value = max(0.0, value * (0.95 + jitter))
                return {"value": round(value, 4), "units": layer.get("units")}

            cells = self._build_grid_cells(bbox, cell_size_m, build_payload)

        quality_summary = self._quality_flags_summary(rows)
        last_sync_at = self._source_last_sync(db, source)
        coverage = self._coverage_percent(range_start, range_end, len(rows), len(LAYER_METRIC_MAP.get(layer_id, ("x",))))
        data = {
            "layer_id": layer_id,
            "source": source,
            "bbox": bbox,
            "zoom": zoom,
            "field_id": field_id,
            "granularity": granularity,
            "agg": agg,
            "time": {
                "from": _iso_utc(range_start),
                "to": _iso_utc(range_end),
            },
            "grid": {
                "cell_size_m": cell_size_m,
                "coverage": 0 if no_data else coverage,
                "cells": cells,
            },
            "meta": {
                "source": source,
                "last_sync_at": last_sync_at,
                "quality_flags_summary": quality_summary,
                "spatial_mode": spatial_mode,
                "zoom_detail": {
                    "zoom": zoom,
                    "mode": spatial_mode,
                    "cell_size_m": cell_size_m,
                },
                "contract_version": CONTRACT_VERSION,
            },
        }
        return data, no_data

    def _get_layer_field(
        self,
        db: DbClient,
        user: UserContext,
        layer_id: str,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        layer = self._load_layer(db, layer_id, source)
        field_id = self._int_value(self._query_str(query, "field_id", required=True), "field_id", min_value=1)
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        range_start, range_end = self._resolve_time_range(query)
        granularity = self._query_str(query, "granularity", required=False) or str(layer.get("default_granularity") or "day")
        allowed_granularities = {str(item) for item in (layer.get("time_available") or [])}
        if granularity not in allowed_granularities:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: granularity", status=422)

        agg = self._query_str(query, "agg", required=False) or "mean"
        if agg not in SCALAR_AGGREGATIONS:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: agg", status=422)

        rows = self._load_layer_points(
            db,
            source=source,
            layer_id=layer_id,
            range_start=range_start,
            range_end=range_end,
            field_id=field_id,
            enterprise_id=(None if user.role_code == ROLE_ADMIN else user.enterprise_id),
        )
        no_data = len(rows) == 0

        values_by_metric: dict[str, list[float]] = {}
        for row in rows:
            metric = str(row.get("metric") or "")
            value = row.get("value")
            if metric not in values_by_metric:
                values_by_metric[metric] = []
            if value is not None:
                values_by_metric[metric].append(float(value))

        aggregated_by_metric: dict[str, float] = {}
        for metric_name, metric_values in values_by_metric.items():
            if metric_values:
                aggregated_by_metric[metric_name] = self._aggregate_values(metric_values, agg)

        value = 0.0 if no_data else self._layer_scalar_value(layer_id, aggregated_by_metric, 0, 0)
        quality_summary = self._quality_flags_summary(rows)
        coverage = self._coverage_percent(range_start, range_end, len(rows), len(LAYER_METRIC_MAP.get(layer_id, ("x",))))
        last_sync_at = self._source_last_sync(db, source)

        return (
            {
                "layer_id": layer_id,
                "field_id": field_id,
                "source": source,
                "granularity": granularity,
                "agg": agg,
                "time": {
                    "from": _iso_utc(range_start),
                    "to": _iso_utc(range_end),
                },
                "value": round(float(value), 4),
                "units": layer.get("units"),
                "meta": {
                    "source": source,
                    "last_sync_at": last_sync_at,
                    "data_coverage": 0.0 if no_data else coverage,
                    "quality_flags_summary": quality_summary,
                    "spatial_mode": "field",
                    "contract_version": CONTRACT_VERSION,
                },
            },
            no_data,
        )

    def _get_layer_tile(
        self,
        db: DbClient,
        user: UserContext,
        *,
        layer_id: str,
        z: int,
        x: int,
        y: int,
        query: dict[str, list[str]],
        request_headers: dict[str, str],
        request_id: str,
    ) -> ApiHttpResponse:
        if z < 0 or z > 22 or x < 0 or y < 0:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: z/x/y", status=422)

        min_lon, min_lat, max_lon, max_lat = self._tile_bbox(z, x, y)
        grid_query = dict(query)
        grid_query["bbox"] = [f"{min_lon},{min_lat},{max_lon},{max_lat}"]
        grid_query["zoom"] = [str(z)]
        data, no_data = self._get_layer_grid(db, user, layer_id, grid_query)

        payload = {
            "tile": {"z": z, "x": x, "y": y},
            "layer_id": layer_id,
            "no_data": no_data,
            "payload": data,
        }
        body = self._json_bytes(payload)
        etag = hashlib.sha256(body).hexdigest()

        if_none_match = self._header(request_headers, "if-none-match")
        if if_none_match and if_none_match == etag:
            return ApiHttpResponse(
                status_code=304,
                body=b"",
                headers={
                    "ETag": etag,
                    "Cache-Control": "public, max-age=120",
                },
                user_id=user.user_id,
            )

        response_headers = {
            "ETag": etag,
            "Cache-Control": "public, max-age=120",
            "Vary": "Accept-Encoding",
        }

        content_encoding = None
        accept_encoding = (self._header(request_headers, "accept-encoding") or "").lower()
        if "gzip" in accept_encoding:
            body = gzip.compress(body, compresslevel=5)
            content_encoding = "gzip"

        if content_encoding:
            response_headers["Content-Encoding"] = content_encoding

        return ApiHttpResponse(
            status_code=200,
            body=body,
            content_type="application/json; charset=utf-8",
            headers=response_headers,
            user_id=user.user_id,
        )

    def _probe_field_layers(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        lat = float(self._str_value(self._query_str(query, "lat", required=True), "lat", min_len=1, max_len=32))
        lon = float(self._str_value(self._query_str(query, "lon", required=True), "lon", min_len=1, max_len=32))
        at = self._parse_datetime(self._query_str(query, "time", required=True))
        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")

        if not self._point_in_field(db, field_id, lon, lat):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: точка вне поля", status=422)

        layers_raw = self._query_str(query, "layers", required=True)
        layer_ids = [item.strip() for item in layers_raw.split(",") if item.strip()]
        if not layer_ids:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: layers", status=422)

        values: list[dict[str, Any]] = []
        for layer_id in layer_ids:
            if layer_id not in LAYER_METRIC_MAP:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: неизвестный layer_id", status=422)
            layer = self._load_layer(db, layer_id, source)
            probe_value = self._probe_layer_value(db, field_id, source, layer_id, at)
            if probe_value is None:
                continue
            values.append(
                {
                    "layer_id": layer_id,
                    "value": probe_value.get("value"),
                    "units": layer.get("units"),
                    "quality": probe_value.get("quality"),
                    "timestamp": probe_value.get("timestamp"),
                    "source": source,
                }
            )

        precipitation_24h = self._metric_aggregate(
            db,
            field_id=field_id,
            source=source,
            metric="precipitation",
            range_start=at - timedelta(hours=24),
            range_end=at,
            agg="sum",
        )
        wind_avg_6h = self._metric_aggregate(
            db,
            field_id=field_id,
            source=source,
            metric="wind_speed",
            range_start=at - timedelta(hours=6),
            range_end=at,
            agg="mean",
        )

        quality_labels = [str(item.get("quality", "")) for item in values if isinstance(item, dict)]
        data_quality = "low" if "low" in quality_labels else "good"

        last_sync_at = self._source_last_sync(db, source)
        freshness_note = ""
        if last_sync_at:
            try:
                sync_dt = self._parse_datetime(last_sync_at)
                stale_hours = (datetime.now(timezone.utc) - sync_dt).total_seconds() / 3600.0
                if stale_hours > 12:
                    data_quality = "low"
                    freshness_note = f" Данные устарели ({int(stale_hours)} ч)."
            except Exception:
                freshness_note = ""

        if wind_avg_6h is not None and wind_avg_6h > 8:
            mini_reco = f"Ветер {wind_avg_6h:.1f} м/с: опрыскивание лучше отложить.{freshness_note}".strip()
        elif precipitation_24h is not None and precipitation_24h > 15:
            mini_reco = f"Осадки за 24ч: {precipitation_24h:.1f} мм, проверьте план полевых работ.{freshness_note}".strip()
        else:
            mini_reco = f"Критичных отклонений нет, продолжайте мониторинг слоя.{freshness_note}".strip()

        no_data = len(values) == 0
        data = {
            "field_id": field_id,
            "probe": {"lat": lat, "lon": lon, "time": _iso_utc(at)},
            "values": values,
            "mini_stats": {
                "precipitation_sum_24h_mm": round(float(precipitation_24h or 0.0), 4),
                "wind_avg_6h_ms": round(float(wind_avg_6h or 0.0), 4),
            },
            "mini_reco": mini_reco,
            "mini_reco_context": {
                "data_quality": data_quality,
                "trigger_factors": {
                    "wind_avg_6h_ms": round(float(wind_avg_6h or 0.0), 4),
                    "precipitation_sum_24h_mm": round(float(precipitation_24h or 0.0), 4),
                },
            },
            "meta": {
                "last_sync_at": last_sync_at,
                "source": source,
            },
        }
        return data, no_data

    def _get_field_zones(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        method = self._query_str(query, "method", required=False) or "grid"
        if method not in {"grid", "quantiles", "kmeans"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: method", status=422)

        zoom = self._int_value(self._query_str(query, "zoom", required=True), "zoom", min_value=0, max_value=22)
        generated_at = self._parse_datetime(self._query_str(query, "time", required=False) or _iso_utc(datetime.now(timezone.utc)))
        generated_hour = generated_at.replace(minute=0, second=0, microsecond=0)

        bbox = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    ST_XMin(bbox) AS min_lon,
                    ST_YMin(bbox) AS min_lat,
                    ST_XMax(bbox) AS max_lon,
                    ST_YMax(bbox) AS max_lat
                FROM fields
                WHERE id = {field_id}
            ) x;
            """
        )
        if not isinstance(bbox, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)

        base_ndvi = self._metric_aggregate(
            db,
            field_id=field_id,
            source=source,
            metric="ndvi",
            range_start=generated_hour - timedelta(days=2),
            range_end=generated_hour,
            agg="mean",
        ) or 0.4

        side = 2 if zoom <= 9 else (3 if zoom <= 12 else (4 if zoom <= 14 else 5))
        min_lon = float(bbox["min_lon"])
        min_lat = float(bbox["min_lat"])
        max_lon = float(bbox["max_lon"])
        max_lat = float(bbox["max_lat"])
        step_lon = (max_lon - min_lon) / side
        step_lat = (max_lat - min_lat) / side

        raw_zones: list[dict[str, Any]] = []
        for iy in range(side):
            for ix in range(side):
                lon1 = min_lon + ix * step_lon
                lon2 = min(max_lon, lon1 + step_lon)
                lat1 = min_lat + iy * step_lat
                lat2 = min(max_lat, lat1 + step_lat)
                weight = 0.85 + ((ix + iy + 1) / max(1, side * side)) * 0.3
                zone_value = max(0.05, min(1.0, base_ndvi * weight))
                raw_zones.append(
                    {
                        "lon1": lon1,
                        "lon2": lon2,
                        "lat1": lat1,
                        "lat2": lat2,
                        "mean": round(zone_value, 4),
                    }
                )

        sorted_for_rank = sorted(enumerate(raw_zones), key=lambda item: item[1]["mean"], reverse=True)
        rank_by_index = {idx: rank + 1 for rank, (idx, _) in enumerate(sorted_for_rank)}

        db.exec_checked(
            f"""
            DELETE FROM api_field_zones
            WHERE field_id = {field_id}
              AND source = {_sql_quote(source)}
              AND method = {_sql_quote(method)}
              AND zoom = {zoom}
              AND generated_for = {_sql_quote(_iso_utc(generated_hour))}::timestamptz;
            """
        )

        items: list[dict[str, Any]] = []
        for idx, zone in enumerate(raw_zones):
            zone_id = uuid.uuid4().hex
            zone_rank = rank_by_index[idx]
            lon1 = zone["lon1"]
            lon2 = zone["lon2"]
            lat1 = zone["lat1"]
            lat2 = zone["lat2"]
            polygon_geojson = {
                "type": "Polygon",
                "coordinates": [[[lon1, lat1], [lon2, lat1], [lon2, lat2], [lon1, lat2], [lon1, lat1]]],
            }
            stats = {
                "mean": zone["mean"],
                "p10": round(max(0.0, zone["mean"] - 0.08), 4),
                "p90": round(min(1.0, zone["mean"] + 0.08), 4),
                "range": 0.16,
            }
            heterogeneity = {
                "cv": round(0.1 + zone_rank * 0.02, 4),
                "p10": stats["p10"],
                "p90": stats["p90"],
                "range": stats["range"],
            }

            wkt = (
                f"POLYGON(({lon1} {lat1},{lon2} {lat1},{lon2} {lat2},"
                f"{lon1} {lat2},{lon1} {lat1}))"
            )
            db.exec_checked(
                f"""
                INSERT INTO api_field_zones (
                    zone_id,
                    field_id,
                    source,
                    method,
                    zoom,
                    zone_rank,
                    zone_geom,
                    heterogeneity,
                    stats,
                    generated_for
                ) VALUES (
                    {_sql_quote(zone_id)},
                    {field_id},
                    {_sql_quote(source)},
                    {_sql_quote(method)},
                    {zoom},
                    {zone_rank},
                    ST_GeomFromText({_sql_quote(wkt)}, 4326),
                    {_sql_quote(json.dumps(heterogeneity, ensure_ascii=False))}::jsonb,
                    {_sql_quote(json.dumps(stats, ensure_ascii=False))}::jsonb,
                    {_sql_quote(_iso_utc(generated_hour))}::timestamptz
                );
                """
            )
            items.append(
                {
                    "zone_id": zone_id,
                    "zone_rank": zone_rank,
                    "polygon": polygon_geojson,
                    "stats": stats,
                    "heterogeneity": heterogeneity,
                }
            )

        items.sort(key=lambda item: item["zone_rank"])
        return {
            "field_id": field_id,
            "source": source,
            "method": method,
            "zoom": zoom,
            "time": _iso_utc(generated_hour),
            "zones": items,
            "meta": {
                "spatial_mode": "zones",
                "zoom_detail": {
                    "zoom": zoom,
                    "mode": "zones",
                    "grid_cells_per_side": side,
                },
            },
        }

    def _get_field_zonal_stats(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[dict[str, Any], bool]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        zone_id = self._query_str(query, "zone_id", required=False)
        metrics_raw = self._query_str(query, "metrics", required=False) or "ndvi,ndre,ndmi"
        metrics = [item.strip() for item in metrics_raw.split(",") if item.strip()]
        valid_metrics = WEATHER_METRICS | SATELLITE_METRICS | {"cloud_mask"}
        for metric in metrics:
            if metric not in valid_metrics:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: metrics", status=422)

        zones = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(z)), '[]'::json)
            FROM (
                SELECT
                    zone_id,
                    zone_rank,
                    stats,
                    heterogeneity
                FROM api_field_zones
                WHERE field_id = {field_id}
                {f"AND zone_id = {_sql_quote(zone_id)}" if zone_id else ""}
                ORDER BY created_at DESC
                LIMIT {1 if zone_id else 8}
            ) z;
            """
        )
        assert isinstance(zones, list)
        if not zones:
            raise ApiError("NOT_FOUND", "Объект не найден", status=404, details="Зоны не сформированы")

        metrics_sql = ", ".join(_sql_quote(metric) for metric in metrics)
        aggregate_rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
            FROM (
                SELECT
                    metric_code AS metric,
                    ROUND(AVG(value)::numeric, 4)::double precision AS mean,
                    ROUND(MIN(value)::numeric, 4)::double precision AS min,
                    ROUND(MAX(value)::numeric, 4)::double precision AS max
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
                  AND metric_code IN ({metrics_sql})
                GROUP BY metric_code
            ) x;
            """
        )
        assert isinstance(aggregate_rows, list)
        by_metric = {str(row.get("metric")): row for row in aggregate_rows if isinstance(row, dict)}

        stats_items: list[dict[str, Any]] = []
        for zone in zones:
            if not isinstance(zone, dict):
                continue
            rank = int(zone.get("zone_rank") or 1)
            factor = 1.0 + (rank - 1) * 0.03
            metric_values: list[dict[str, Any]] = []
            for metric in metrics:
                base = by_metric.get(metric)
                if not base:
                    continue
                mean = float(base.get("mean") or 0.0) * factor
                p10 = float(base.get("min") or 0.0) * factor
                p90 = float(base.get("max") or 0.0) * factor
                metric_values.append(
                    {
                        "metric": metric,
                        "mean": round(mean, 4),
                        "p10": round(p10, 4),
                        "p90": round(p90, 4),
                        "range": round(max(0.0, p90 - p10), 4),
                    }
                )
            stats_items.append(
                {
                    "zone_id": zone.get("zone_id"),
                    "zone_rank": rank,
                    "heterogeneity": zone.get("heterogeneity") or {},
                    "metrics": metric_values,
                }
            )

        no_data = len(aggregate_rows) == 0
        return (
            {
                "field_id": field_id,
                "source": source,
                "from": _iso_utc(range_start),
                "to": _iso_utc(range_end),
                "items": stats_items,
            },
            no_data,
        )

    def _stream_events(self, db: DbClient, user: UserContext, request_id: str) -> ApiHttpResponse:
        sync_rows = db.query_json(
            """
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.source), '[]'::json)
            FROM (
                SELECT
                    source,
                    status,
                    CASE
                        WHEN last_success_at IS NULL THEN to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                        ELSE to_char(last_success_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_sync_at,
                    last_error
                FROM provider_sync_status
            ) x;
            """
        )
        assert isinstance(sync_rows, list)

        export_rows = db.query_json(
            """
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.updated_at DESC), '[]'::json)
            FROM (
                SELECT
                    export_id,
                    status,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM api_export_jobs
                WHERE status = 'done'
                ORDER BY updated_at DESC
                LIMIT 5
            ) x;
            """
        )
        assert isinstance(export_rows, list)

        scenario_rows = db.query_json(
            """
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.updated_at DESC), '[]'::json)
            FROM (
                SELECT
                    scenario_id,
                    status,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM api_scenarios
                WHERE status IN ('done', 'failed')
                ORDER BY updated_at DESC
                LIMIT 5
            ) x;
            """
        )
        assert isinstance(scenario_rows, list)

        lines: list[str] = []
        for row in sync_rows:
            if not isinstance(row, dict):
                continue
            lines.append("event: sync_updated")
            lines.append(f"data: {json.dumps(row, ensure_ascii=False)}")
            lines.append("")
            if str(row.get("status") or "").lower() in {"error", "down", "failed"}:
                payload = {
                    "source": row.get("source"),
                    "reason": row.get("last_error") or "Источник недоступен",
                }
                lines.append("event: source_down")
                lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
                lines.append("")

        for row in export_rows:
            if not isinstance(row, dict):
                continue
            lines.append("event: export_ready")
            lines.append(f"data: {json.dumps(row, ensure_ascii=False)}")
            lines.append("")

        for row in scenario_rows:
            if not isinstance(row, dict):
                continue
            lines.append("event: scenario_done")
            lines.append(f"data: {json.dumps(row, ensure_ascii=False)}")
            lines.append("")

        if not lines:
            lines = [
                "event: heartbeat",
                f"data: {json.dumps({'request_id': request_id}, ensure_ascii=False)}",
                "",
            ]

        body = ("\n".join(lines) + "\n").encode("utf-8")
        return ApiHttpResponse(
            status_code=200,
            body=body,
            content_type="text/event-stream; charset=utf-8",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "close",
            },
            user_id=user.user_id,
        )

    # -------------------------------
    # Scenario modeling API
    # -------------------------------
    def _create_scenario(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        field_id = self._int_value(payload.get("field_id"), "field_id", min_value=1)
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        source = self._normalize_source(self._str_value(payload.get("source"), "source", min_len=4, max_len=20))
        range_start = self._parse_datetime(self._str_value(payload.get("from"), "from", min_len=10, max_len=40))
        range_end = self._parse_datetime(self._str_value(payload.get("to"), "to", min_len=10, max_len=40))
        if range_end < range_start:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: диапазон времени", status=422)
        if range_start < (datetime.now(timezone.utc) - timedelta(days=30)):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: диапазон вне доступного хранения", status=422)
        if "baseline_id" in payload and not payload.get("baseline_id"):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: сценарий без baseline недопустим", status=422)
        baseline_id = self._str_value(payload.get("baseline_id") or uuid.uuid4().hex, "baseline_id", min_len=6, max_len=80)
        params = payload.get("params", {})
        validated_params = self._validate_scenario_params(params)

        baseline_count_raw = db.exec_checked(
            f"""
            SELECT COUNT(*)
            FROM provider_observations
            WHERE field_id = {field_id}
              AND source = {_sql_quote(source)}
              AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                  AND {_sql_quote(_iso_utc(range_end))}::timestamptz;
            """,
            tuples_only=True,
        )
        if self._scalar_int(baseline_count_raw) == 0:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: сценарий без baseline недопустим", status=422)

        scenario_id = uuid.uuid4().hex
        db.exec_checked(
            f"""
            INSERT INTO api_scenarios (
                scenario_id,
                baseline_id,
                field_id,
                source,
                range_start,
                range_end,
                params,
                status,
                created_by,
                request_id
            ) VALUES (
                {_sql_quote(scenario_id)},
                {_sql_quote(baseline_id)},
                {field_id},
                {_sql_quote(source)},
                {_sql_quote(_iso_utc(range_start))}::timestamptz,
                {_sql_quote(_iso_utc(range_end))}::timestamptz,
                {_sql_quote(json.dumps(validated_params, ensure_ascii=False))}::jsonb,
                'draft',
                {user.user_id},
                {_sql_quote(request_id)}
            );
            """
        )
        created = self._get_scenario(db, user, scenario_id)
        self._write_audit(db, user.user_id, "scenario.create", "scenario", scenario_id, None, created, request_id)
        return created

    def _update_scenario(
        self,
        db: DbClient,
        user: UserContext,
        scenario_id: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        before = self._load_scenario(db, user, scenario_id)
        if str(before.get("status")) == "running":
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409)

        raw_params = payload.get("params", payload)
        if not isinstance(raw_params, dict):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: params", status=422)
        params = dict(before.get("params") or {})
        params.update(raw_params)
        validated_params = self._validate_scenario_params(params)

        db.exec_checked(
            f"""
            UPDATE api_scenarios
            SET
                params = {_sql_quote(json.dumps(validated_params, ensure_ascii=False))}::jsonb,
                updated_at = NOW()
            WHERE scenario_id = {_sql_quote(scenario_id)};
            """
        )
        after = self._load_scenario(db, user, scenario_id)
        self._write_audit(db, user.user_id, "scenario.update", "scenario", scenario_id, before, after, request_id)
        return self._scenario_public_view(after)

    def _run_scenario(self, db: DbClient, user: UserContext, scenario_id: str, request_id: str) -> dict[str, Any]:
        scenario = self._load_scenario(db, user, scenario_id)
        if str(scenario.get("status")) == "running":
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409)

        db.exec_checked(
            f"""
            UPDATE api_scenarios
            SET status = 'running', updated_at = NOW(), error_text = NULL
            WHERE scenario_id = {_sql_quote(scenario_id)};
            """
        )

        field_id = int(scenario["field_id"])
        source = str(scenario["source"])
        range_start = self._parse_datetime(str(scenario["range_start"]))
        range_end = self._parse_datetime(str(scenario["range_end"]))
        params = scenario.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        baseline_rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
            FROM (
                SELECT
                    metric_code AS metric,
                    ROUND(AVG(value)::numeric, 4)::double precision AS value,
                    MIN(unit) AS unit
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
                GROUP BY metric_code
            ) x;
            """
        )
        assert isinstance(baseline_rows, list)
        if not baseline_rows:
            db.exec_checked(
                f"""
                UPDATE api_scenarios
                SET status = 'failed', error_text = 'Нет baseline-данных в выбранном диапазоне', updated_at = NOW()
                WHERE scenario_id = {_sql_quote(scenario_id)};
                """
            )
            raise ApiError("NO_DATA", "Нет данных за выбранный период", status=200)

        baseline: dict[str, tuple[float, str]] = {}
        for row in baseline_rows:
            if not isinstance(row, dict):
                continue
            metric = str(row.get("metric") or "")
            baseline[metric] = (float(row.get("value") or 0.0), str(row.get("unit") or ""))

        scenario_values, assumptions = self._apply_scenario_params(baseline, params)
        diff_metrics: list[dict[str, Any]] = []
        result_values: list[dict[str, Any]] = []
        for metric, (scenario_value, unit) in scenario_values.items():
            baseline_value = baseline.get(metric, (0.0, unit))[0]
            diff_metrics.append(
                {
                    "metric": metric,
                    "baseline": round(baseline_value, 4),
                    "scenario": round(scenario_value, 4),
                    "delta": round(scenario_value - baseline_value, 4),
                    "unit": unit,
                }
            )
            result_values.append(
                {
                    "metric": metric,
                    "value": round(scenario_value, 4),
                    "unit": unit,
                    "timestamp": _iso_utc(range_end),
                    "source": "scenario",
                    "quality_flags": [],
                    "meta": {
                        "scenario_id": scenario_id,
                        "baseline_id": scenario.get("baseline_id"),
                        "contract_version": CONTRACT_VERSION,
                    },
                }
            )

        def estimate_et0(values_map: dict[str, tuple[float, str]]) -> float:
            t = float(values_map.get("temperature", (0.0, "C"))[0])
            rh = float(values_map.get("humidity_rh", (60.0, "%"))[0])
            wind = float(values_map.get("wind_speed", (2.0, "m/s"))[0])
            rad = float(values_map.get("radiation", (220.0, "W/m2"))[0])
            rh = max(0.0, min(100.0, rh))
            wind = max(0.0, wind)
            rad = max(0.0, rad)
            es = 0.6108 * math.exp((17.27 * t) / (t + 237.3))
            ea = es * (rh / 100.0)
            delta = 4098.0 * es / ((t + 237.3) ** 2)
            gamma = 0.066
            rn = (rad * 0.0864) * 0.77
            numerator = 0.408 * delta * rn + gamma * (900.0 / (t + 273.0)) * wind * max(0.0, es - ea)
            denominator = delta + gamma * (1.0 + 0.34 * wind)
            if denominator <= 0:
                return 0.0
            return max(0.0, numerator / denominator)

        baseline_et0 = estimate_et0(baseline)
        scenario_et0 = estimate_et0(scenario_values)
        baseline_precip = float(baseline.get("precipitation", (0.0, "mm"))[0])
        scenario_precip = float(scenario_values.get("precipitation", (0.0, "mm"))[0])
        baseline_water_deficit = baseline_et0 - baseline_precip
        scenario_water_deficit = scenario_et0 - scenario_precip

        derived_diff = [
            {
                "metric": "et0_mm_day",
                "baseline": round(baseline_et0, 4),
                "scenario": round(scenario_et0, 4),
                "delta": round(scenario_et0 - baseline_et0, 4),
                "unit": "mm/day",
            },
            {
                "metric": "water_deficit_mm",
                "baseline": round(baseline_water_deficit, 4),
                "scenario": round(scenario_water_deficit, 4),
                "delta": round(scenario_water_deficit - baseline_water_deficit, 4),
                "unit": "mm",
            },
        ]

        scenario_recommendations: list[dict[str, Any]] = []
        scenario_wind = float(scenario_values.get("wind_speed", (0.0, "m/s"))[0])
        if scenario_wind > 8.0:
            scenario_recommendations.append(
                {
                    "type": "spraying_window",
                    "what_to_do": "Отложить опрыскивание",
                    "why": f"Ветер {scenario_wind:.2f} м/с превышает безопасный порог 8 м/с",
                    "confidence": "высокое",
                }
            )
        if scenario_water_deficit > 5.0:
            scenario_recommendations.append(
                {
                    "type": "irrigation",
                    "what_to_do": "Планировать полив",
                    "why": f"Водный дефицит {scenario_water_deficit:.2f} мм",
                    "confidence": "среднее",
                }
            )

        result_payload = {
            "source": "scenario",
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "values": result_values,
            "derived": {
                "et0_mm_day": round(scenario_et0, 4),
                "water_deficit_mm": round(scenario_water_deficit, 4),
            },
            "recommendations": scenario_recommendations,
            "meta": {
                "scenario_id": scenario_id,
                "baseline_id": scenario.get("baseline_id"),
                "assumptions": assumptions,
            },
        }
        diff_payload = {
            "scenario_id": scenario_id,
            "baseline_id": scenario.get("baseline_id"),
            "metrics": diff_metrics,
            "derived_metrics": derived_diff,
            "recommendation_changes": scenario_recommendations,
            "map_hint": "Сравнение доступно для отображения на карте/графиках",
        }

        db.exec_checked(
            f"""
            UPDATE api_scenarios
            SET
                status = 'done',
                result_payload = {_sql_quote(json.dumps(result_payload, ensure_ascii=False))}::jsonb,
                diff_payload = {_sql_quote(json.dumps(diff_payload, ensure_ascii=False))}::jsonb,
                assumptions = {_sql_quote(json.dumps(assumptions, ensure_ascii=False))}::jsonb,
                error_text = NULL,
                updated_at = NOW()
            WHERE scenario_id = {_sql_quote(scenario_id)};
            """
        )
        after = self._load_scenario(db, user, scenario_id)
        self._write_audit(db, user.user_id, "scenario.run", "scenario", scenario_id, scenario, after, request_id)
        return {
            "scenario_id": scenario_id,
            "status": "done",
            "assumptions_count": len(assumptions),
            "result_points": len(result_values),
        }

    def _get_scenario(self, db: DbClient, user: UserContext, scenario_id: str) -> dict[str, Any]:
        scenario = self._load_scenario(db, user, scenario_id)
        return self._scenario_public_view(scenario)

    def _get_scenario_result(self, db: DbClient, user: UserContext, scenario_id: str) -> dict[str, Any]:
        scenario = self._load_scenario(db, user, scenario_id)
        if scenario.get("result_payload") is None:
            return {
                "scenario_id": scenario_id,
                "status": scenario.get("status"),
                "result": None,
                "message": "Результат ещё не готов",
            }
        return {
            "scenario_id": scenario_id,
            "status": scenario.get("status"),
            "result": scenario.get("result_payload"),
        }

    def _get_scenario_diff(self, db: DbClient, user: UserContext, scenario_id: str) -> dict[str, Any]:
        scenario = self._load_scenario(db, user, scenario_id)
        if scenario.get("diff_payload") is None:
            return {
                "scenario_id": scenario_id,
                "status": scenario.get("status"),
                "diff": None,
                "message": "Diff ещё не готов",
            }
        return {
            "scenario_id": scenario_id,
            "status": scenario.get("status"),
            "diff": scenario.get("diff_payload"),
        }

    def _load_layer(self, db: DbClient, layer_id: str, source: str) -> dict[str, Any]:
        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    layer_id,
                    source,
                    title_ru,
                    category,
                    value_type,
                    units,
                    time_available,
                    default_granularity,
                    max_lookback_days,
                    spatial_modes,
                    zoom_rules,
                    grid_sizes_m,
                    legend,
                    has_quality_flags,
                    quality_rules
                FROM api_layer_registry
                WHERE layer_id = {_sql_quote(layer_id)}
                  AND source = {_sql_quote(source)}
                  AND is_active = TRUE
            ) x;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404, details="Слой не найден")
        return payload

    def _resolve_time_range(self, query: dict[str, list[str]]) -> tuple[datetime, datetime]:
        time_value = self._query_str(query, "time", required=False)
        if time_value:
            at = self._parse_datetime(time_value)
            return at, at
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))
        if range_end < range_start:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: диапазон времени", status=422)
        return range_start, range_end

    def _parse_bbox(self, value: str) -> list[float]:
        parts = [item.strip() for item in value.split(",")]
        if len(parts) != 4:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: bbox", status=422)
        try:
            min_lon, min_lat, max_lon, max_lat = [float(item) for item in parts]
        except ValueError as exc:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: bbox", status=422) from exc
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: bbox", status=422)
        return [min_lon, min_lat, max_lon, max_lat]

    def _load_layer_points(
        self,
        db: DbClient,
        *,
        source: str,
        layer_id: str,
        range_start: datetime,
        range_end: datetime,
        field_id: int | None,
        enterprise_id: int | None,
    ) -> list[dict[str, Any]]:
        metrics = LAYER_METRIC_MAP.get(layer_id)
        if not metrics:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: layer_id", status=422)
        metrics_sql = ", ".join(_sql_quote(metric) for metric in metrics)
        field_sql = f"AND field_id = {field_id}" if field_id is not None else ""
        enterprise_sql = (
            f"AND field_id IN (SELECT id FROM fields WHERE enterprise_id = {enterprise_id})"
            if enterprise_id is not None and field_id is None
            else ""
        )

        rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.timestamp DESC), '[]'::json)
            FROM (
                SELECT
                    metric_code AS metric,
                    ROUND(value::numeric, 4)::double precision AS value,
                    unit,
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    quality_flags
                FROM provider_observations
                WHERE source = {_sql_quote(source)}
                  {field_sql}
                  {enterprise_sql}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
                  AND metric_code IN ({metrics_sql})
            ) x;
            """
        )
        assert isinstance(rows, list)
        return rows

    def _aggregate_values(self, values: list[float], agg: str) -> float:
        if not values:
            return 0.0
        sorted_values = sorted(values)
        if agg == "sum":
            return float(sum(values))
        if agg == "min":
            return float(sorted_values[0])
        if agg == "max":
            return float(sorted_values[-1])
        if agg == "median":
            mid = len(sorted_values) // 2
            if len(sorted_values) % 2 == 1:
                return float(sorted_values[mid])
            return float((sorted_values[mid - 1] + sorted_values[mid]) / 2.0)
        if agg in {"p10", "p90"}:
            p = 0.1 if agg == "p10" else 0.9
            idx = int(round((len(sorted_values) - 1) * p))
            idx = max(0, min(len(sorted_values) - 1, idx))
            return float(sorted_values[idx])
        return float(sum(values) / len(values))

    def _build_grid_cells(
        self,
        bbox: list[float],
        cell_size_m: int,
        payload_builder: Any,
    ) -> list[dict[str, Any]]:
        min_lon, min_lat, max_lon, max_lat = bbox
        cell_size_deg = max(0.0001, float(cell_size_m) / 111_320.0)
        count_x = max(1, int(math.ceil((max_lon - min_lon) / cell_size_deg)))
        count_y = max(1, int(math.ceil((max_lat - min_lat) / cell_size_deg)))
        if count_x * count_y > 2500:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: слишком крупный bbox для выбранного zoom", status=422)

        cells: list[dict[str, Any]] = []
        for iy in range(count_y):
            for ix in range(count_x):
                lon1 = min_lon + ix * cell_size_deg
                lon2 = min(max_lon, lon1 + cell_size_deg)
                lat1 = min_lat + iy * cell_size_deg
                lat2 = min(max_lat, lat1 + cell_size_deg)
                center_lon = (lon1 + lon2) / 2.0
                center_lat = (lat1 + lat2) / 2.0
                payload = payload_builder(ix, iy, center_lon, center_lat)
                cells.append(
                    {
                        "cell_id": f"{ix}:{iy}",
                        "bbox": [round(lon1, 7), round(lat1, 7), round(lon2, 7), round(lat2, 7)],
                        "center": {"lon": round(center_lon, 7), "lat": round(center_lat, 7)},
                        **payload,
                    }
                )
        return cells

    def _quality_flags_summary(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        summary: dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            for flag in row.get("quality_flags", []) or []:
                key = str(flag)
                summary[key] = summary.get(key, 0) + 1
        return summary

    @staticmethod
    def _tile_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
        n = 2.0 ** z
        min_lon = x / n * 360.0 - 180.0
        max_lon = (x + 1) / n * 360.0 - 180.0
        lat_rad_top = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_rad_bottom = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        max_lat = math.degrees(lat_rad_top)
        min_lat = math.degrees(lat_rad_bottom)
        return min_lon, min_lat, max_lon, max_lat

    def _source_last_sync(self, db: DbClient, source: str) -> str | None:
        status = get_sync_status(db, source)
        value = status.get("last_success_at") or status.get("last_sync_at")
        return str(value) if value else None

    @staticmethod
    def _source_status_label(sync_status: str, fallback: str) -> str:
        normalized = sync_status.lower()
        if normalized in {"ok", "ready"}:
            return "OK"
        if normalized in {"error", "degraded", "warning"}:
            return "DEGRADED"
        if normalized in {"down", "failed"}:
            return "DOWN"
        return fallback if fallback in {"OK", "DEGRADED", "DOWN"} else "OK"

    @staticmethod
    def _nearest_size(sizes: list[int], target: int) -> int:
        if not sizes:
            return target
        return min(sizes, key=lambda item: abs(item - target))

    def _select_spatial_mode(self, layer: dict[str, Any], zoom: int, *, prefer: str) -> tuple[str, int]:
        modes = {str(item) for item in (layer.get("spatial_modes") or ["grid"])}
        sizes = sorted({int(x) for x in (layer.get("grid_sizes_m") or [1000])}, reverse=True)
        if not sizes:
            sizes = [1000]

        if zoom <= 9:
            mode = "field" if "field" in modes and prefer != "grid" else "grid"
            target = 1000
        elif zoom <= 12:
            mode = "grid"
            target = 500
        elif zoom <= 14:
            mode = "grid"
            target = 250
        else:
            if "zones" in modes and prefer == "zones":
                mode = "zones"
            else:
                mode = "grid"
            target = 100

        if mode not in modes:
            mode = "grid" if "grid" in modes else sorted(modes)[0]

        cell_size = self._nearest_size(sizes, target)
        return mode, int(cell_size)

    def _select_cell_size(self, layer: dict[str, Any], zoom: int) -> int:
        _, size = self._select_spatial_mode(layer, zoom, prefer="grid")
        return size

    def _layer_scalar_value(
        self,
        layer_id: str,
        aggregated_by_metric: dict[str, float],
        ix: int,
        iy: int,
    ) -> float:
        base_metric = LAYER_METRIC_MAP.get(layer_id, ("",))[0]
        base_value = float(aggregated_by_metric.get(base_metric, 0.0))
        ndvi = float(aggregated_by_metric.get("ndvi", 0.0))
        cloud = float(aggregated_by_metric.get("cloud_total", aggregated_by_metric.get("cloudiness", 0.0)))
        soil = float(aggregated_by_metric.get("soil_moisture", 0.0))
        precip = float(aggregated_by_metric.get("precipitation", 0.0))
        wind = float(aggregated_by_metric.get("wind_speed", 0.0))
        jitter = ((ix + 3) * (iy + 5)) % 7 / 100.0

        if layer_id == "weather.vorticity_index":
            return max(0.0, min(1.0, wind / 20.0 + jitter))
        if layer_id == "soil.moisture_anomaly":
            return max(-50.0, min(50.0, soil - 35.0))
        if layer_id == "soil.trafficability_risk":
            risk = (soil / 100.0) * 0.6 + min(1.0, precip / 20.0) * 0.4
            return max(0.0, min(1.0, risk))
        if layer_id in {"sat.scene_quality", "sat.cloud_mask"}:
            return max(0.0, min(100.0, cloud))
        if layer_id == "sat.growth_rate":
            return max(-0.2, min(0.2, ndvi - 0.45))
        if layer_id == "sat.field_uniformity_cv":
            return max(0.0, min(1.0, 0.12 + jitter * 4))
        if layer_id == "sat.anomaly_vs_baseline":
            return max(-1.0, min(1.0, ndvi - 0.45))
        if layer_id == "sat.season_curve":
            return max(0.0, min(1.0, ndvi))

        return base_value

    def _point_in_field(self, db: DbClient, field_id: int, lon: float, lat: float) -> bool:
        payload = db.query_json(
            f"""
            SELECT to_json(COALESCE((
                SELECT ST_Contains(
                    geom,
                    ST_SetSRID(ST_Point({lon}, {lat}), 4326)
                )
                FROM fields
                WHERE id = {field_id}
                  AND deleted_at IS NULL
            ), FALSE));
            """
        )
        return bool(payload)

    def _probe_layer_value(
        self,
        db: DbClient,
        field_id: int,
        source: str,
        layer_id: str,
        at: datetime,
    ) -> dict[str, Any] | None:
        metrics = LAYER_METRIC_MAP.get(layer_id)
        if not metrics:
            return None
        metrics_sql = ", ".join(_sql_quote(metric) for metric in metrics)
        rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
            FROM (
                SELECT
                    metric_code AS metric,
                    ROUND(value::numeric, 4)::double precision AS value,
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    quality_flags
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND metric_code IN ({metrics_sql})
                  AND observed_at <= {_sql_quote(_iso_utc(at))}::timestamptz
                ORDER BY observed_at DESC
                LIMIT 5
            ) x;
            """
        )
        assert isinstance(rows, list)
        if not rows:
            return None

        values = [float(item.get("value") or 0.0) for item in rows if isinstance(item, dict)]
        if not values:
            return None
        flags = rows[0].get("quality_flags") if isinstance(rows[0], dict) else []
        quality = "low" if any(str(flag) in {"cloudy", "low_confidence"} for flag in (flags or [])) else "good"
        return {
            "value": round(sum(values) / len(values), 4),
            "timestamp": rows[0].get("timestamp"),
            "quality": quality,
        }

    def _metric_aggregate(
        self,
        db: DbClient,
        *,
        field_id: int,
        source: str,
        metric: str,
        range_start: datetime,
        range_end: datetime,
        agg: str,
    ) -> float | None:
        if agg not in {"sum", "mean", "min", "max"}:
            agg = "mean"
        sql_agg = {"sum": "SUM", "mean": "AVG", "min": "MIN", "max": "MAX"}[agg]
        payload = db.query_json(
            f"""
            SELECT to_json((
                SELECT ROUND({sql_agg}(value)::numeric, 4)::double precision
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND metric_code = {_sql_quote(metric)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
            ));
            """
        )
        if payload is None:
            return None
        return float(payload)

    def _load_scenario(self, db: DbClient, user: UserContext, scenario_id: str) -> dict[str, Any]:
        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    s.scenario_id,
                    s.baseline_id,
                    s.field_id,
                    s.source,
                    to_char(s.range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                    to_char(s.range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                    s.params,
                    s.status,
                    s.result_payload,
                    s.diff_payload,
                    s.assumptions,
                    s.error_text,
                    s.created_by,
                    to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(s.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
                    f.enterprise_id
                FROM api_scenarios s
                JOIN fields f ON f.id = s.field_id
                WHERE s.scenario_id = {_sql_quote(scenario_id)}
            ) x;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        self._assert_enterprise_scope(user, int(payload["enterprise_id"]))
        return payload

    @staticmethod
    def _scenario_public_view(scenario: dict[str, Any]) -> dict[str, Any]:
        return {
            "scenario_id": scenario.get("scenario_id"),
            "baseline_id": scenario.get("baseline_id"),
            "field_id": scenario.get("field_id"),
            "source": scenario.get("source"),
            "from": scenario.get("range_start"),
            "to": scenario.get("range_end"),
            "params": scenario.get("params") or {},
            "status": scenario.get("status"),
            "assumptions": scenario.get("assumptions") or [],
            "error": scenario.get("error_text"),
            "created_at": scenario.get("created_at"),
            "updated_at": scenario.get("updated_at"),
        }

    def _validate_scenario_params(self, params: Any) -> dict[str, Any]:
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: params", status=422)

        allowed = {
            "rain_delta_mm",
            "duration_hours",
            "temp_shift_c",
            "wind_shift_ms",
            "irrigation_event",
            "fertilizer_event",
            "operation_shift",
        }
        for key in params:
            if key not in allowed:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: неизвестный параметр сценария", status=422)

        validated = dict(params)
        if "rain_delta_mm" in validated:
            rain = float(validated["rain_delta_mm"])
            if rain < 0 or rain > 100:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: rain_delta_mm", status=422)
            validated["rain_delta_mm"] = rain

        if "duration_hours" in validated:
            duration = int(validated["duration_hours"])
            if duration < 1 or duration > 168:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: duration_hours", status=422)
            validated["duration_hours"] = duration

        if "temp_shift_c" in validated:
            temp_shift = float(validated["temp_shift_c"])
            if temp_shift < -20 or temp_shift > 20:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: temp_shift_c", status=422)
            validated["temp_shift_c"] = temp_shift

        if "wind_shift_ms" in validated:
            wind_shift = float(validated["wind_shift_ms"])
            if wind_shift < -20 or wind_shift > 20:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: wind_shift_ms", status=422)
            validated["wind_shift_ms"] = wind_shift

        irrigation = validated.get("irrigation_event")
        if irrigation is not None:
            if not isinstance(irrigation, dict):
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: irrigation_event", status=422)
            mm = float(irrigation.get("mm", 0))
            if mm < 0 or mm > 100:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: irrigation_event.mm", status=422)

        fertilizer = validated.get("fertilizer_event")
        if fertilizer is not None:
            if not isinstance(fertilizer, dict):
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: fertilizer_event", status=422)
            rate = float(fertilizer.get("rate", 0))
            if rate < 0 or rate > 1000:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: fertilizer_event.rate", status=422)
            fertilizer_type = fertilizer.get("type")
            if fertilizer_type is not None and len(str(fertilizer_type).strip()) < 2:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: fertilizer_event.type", status=422)

        operation_shift = validated.get("operation_shift")
        if operation_shift is not None:
            if not isinstance(operation_shift, dict):
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: operation_shift", status=422)
            days = int(operation_shift.get("days", 0))
            if days < -30 or days > 30:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: operation_shift.days", status=422)

        return validated

    def _apply_scenario_params(
        self,
        baseline: dict[str, tuple[float, str]],
        params: dict[str, Any],
    ) -> tuple[dict[str, tuple[float, str]], list[str]]:
        scenario = dict(baseline)
        assumptions: list[str] = []

        rain_delta = float(params.get("rain_delta_mm", 0.0) or 0.0)
        if rain_delta:
            value, unit = scenario.get("precipitation", (0.0, "mm"))
            scenario["precipitation"] = (value + rain_delta, unit or "mm")
            assumptions.append(f"Добавлено осадков: +{rain_delta:.2f} мм")

        temp_shift = float(params.get("temp_shift_c", 0.0) or 0.0)
        if temp_shift:
            value, unit = scenario.get("temperature", (0.0, "C"))
            scenario["temperature"] = (value + temp_shift, unit or "C")
            assumptions.append(f"Смещение температуры: {temp_shift:+.2f} C")

        wind_shift = float(params.get("wind_shift_ms", 0.0) or 0.0)
        if wind_shift:
            value, unit = scenario.get("wind_speed", (0.0, "m/s"))
            scenario["wind_speed"] = (max(0.0, value + wind_shift), unit or "m/s")
            assumptions.append(f"Смещение скорости ветра: {wind_shift:+.2f} м/с")

        irrigation = params.get("irrigation_event")
        if isinstance(irrigation, dict):
            irrigation_mm = float(irrigation.get("mm", 0.0) or 0.0)
            if irrigation_mm:
                value, unit = scenario.get("precipitation", (0.0, "mm"))
                scenario["precipitation"] = (value + irrigation_mm, unit or "mm")
                assumptions.append(f"Учтён полив: +{irrigation_mm:.2f} мм")

        fertilizer = params.get("fertilizer_event")
        if isinstance(fertilizer, dict):
            value_ndvi, unit_ndvi = scenario.get("ndvi", (0.0, "index"))
            value_ndre, unit_ndre = scenario.get("ndre", (0.0, "index"))
            scenario["ndvi"] = (min(1.0, value_ndvi + 0.03), unit_ndvi or "index")
            scenario["ndre"] = (min(1.0, value_ndre + 0.02), unit_ndre or "index")
            assumptions.append("Упрощённое влияние удобрений применено к NDVI/NDRE")

        operation_shift = params.get("operation_shift")
        if isinstance(operation_shift, dict):
            shift_days = int(operation_shift.get("days", 0) or 0)
            assumptions.append(f"Сдвиг операции: {shift_days:+d} дней (учтён в допущениях)")

        for metric, (value, unit) in list(scenario.items()):
            if unit == "%":
                scenario[metric] = (max(0.0, min(100.0, value)), unit)
            if unit == "index":
                scenario[metric] = (max(0.0, min(1.0, value)), unit)
            if metric == "wind_speed":
                scenario[metric] = (max(0.0, value), unit)

        if not assumptions:
            assumptions.append("Сценарий без изменений параметров: baseline без модификаций")

        return scenario, assumptions

    # -------------------------------
    # Stage6 algorithms (derived block)
    # -------------------------------
    def _load_metric_rows(
        self,
        db: DbClient,
        *,
        field_id: int,
        source: str,
        metrics: list[str],
        range_start: datetime,
        range_end: datetime,
    ) -> list[dict[str, Any]]:
        metrics_sql = ", ".join(_sql_quote(metric) for metric in metrics)
        rows = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.observed_at), '[]'::json)
            FROM (
                SELECT
                    metric_code AS metric,
                    ROUND(value::numeric, 6)::double precision AS value,
                    unit,
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    quality_flags,
                    observed_at
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND source = {_sql_quote(source)}
                  AND metric_code IN ({metrics_sql})
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
            ) x;
            """
        )
        assert isinstance(rows, list)
        return rows

    @staticmethod
    def _rows_by_metric(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            metric = str(row.get("metric") or "")
            grouped.setdefault(metric, []).append(row)
        return grouped

    def _validate_metric_units(self, rows_by_metric: dict[str, list[dict[str, Any]]], metrics: list[str]) -> None:
        for metric in metrics:
            expected = EXPECTED_UNITS.get(metric)
            if expected is None:
                continue
            series = rows_by_metric.get(metric) or []
            if not series:
                continue
            unit = str(series[0].get("unit") or "")
            if unit != expected:
                raise ApiError(
                    "VALIDATION_ERROR",
                    f"Некорректные входные данные: единицы {metric} ({unit}), ожидается {expected}",
                    status=422,
                )

    def _algorithm_quality_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        flags = self._quality_flags_summary(rows)
        total_points = len(rows)
        low_flags = 0
        for flag_name, count in flags.items():
            if flag_name in {"low_confidence", "cloudy", "cloud_mask_interpolated", "low_quality"}:
                low_flags += int(count)
        confidence = "high"
        if total_points > 0 and (low_flags / total_points) > 0.2:
            confidence = "low"
        elif total_points > 0 and (low_flags / total_points) > 0.05:
            confidence = "medium"
        return {
            "total_points": total_points,
            "quality_flags": flags,
            "low_quality_points": low_flags,
            "confidence": confidence,
        }

    def _algorithm_raw_block(
        self,
        db: DbClient,
        *,
        source: str,
        range_start: datetime,
        range_end: datetime,
        rows: list[dict[str, Any]],
        metric_count: int,
    ) -> dict[str, Any]:
        coverage = self._coverage_percent(range_start, range_end, len(rows), metric_count)
        return {
            "source": source,
            "last_sync_at": self._source_last_sync(db, source),
            "data_coverage": coverage,
            "quality_flags": self._quality_flags_summary(rows),
            "values": rows,
        }

    def _store_algorithm_run(
        self,
        db: DbClient,
        *,
        field_id: int,
        source: str,
        algorithm_id: str,
        range_start: datetime,
        range_end: datetime,
        status: str,
        reason: str | None,
        error_code: str | None,
        inputs_used: dict[str, Any],
        quality_summary: dict[str, Any],
        result_payload: dict[str, Any] | None,
        user_id: int | None,
        request_id: str,
    ) -> None:
        run_id = uuid.uuid4().hex
        db.exec_checked(
            f"""
            INSERT INTO api_algorithm_runs (
                run_id,
                field_id,
                source,
                algorithm_id,
                algorithm_version,
                range_start,
                range_end,
                status,
                reason,
                error_code,
                inputs_used,
                quality_summary,
                result_payload,
                created_by,
                request_id
            ) VALUES (
                {_sql_quote(run_id)},
                {field_id},
                {_sql_quote(source)},
                {_sql_quote(algorithm_id)},
                {_sql_quote(ALGORITHM_VERSION)},
                {_sql_quote(_iso_utc(range_start))}::timestamptz,
                {_sql_quote(_iso_utc(range_end))}::timestamptz,
                {_sql_quote(status)},
                {(_sql_quote(reason) if reason is not None else 'NULL')},
                {(_sql_quote(error_code) if error_code is not None else 'NULL')},
                {_sql_quote(json.dumps(inputs_used, ensure_ascii=False))}::jsonb,
                {_sql_quote(json.dumps(quality_summary, ensure_ascii=False))}::jsonb,
                {(_sql_quote(json.dumps(result_payload, ensure_ascii=False)) + '::jsonb') if result_payload is not None else 'NULL'},
                {str(user_id) if user_id is not None else 'NULL'},
                {_sql_quote(request_id)}
            );
            """
        )

    def _algorithm_context(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> tuple[str, datetime, datetime]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))
        source = self._normalize_source(self._query_str(query, "source", required=False) or "Copernicus")
        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))
        if range_end < range_start:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: диапазон времени", status=422)
        return source, range_start, range_end

    def _get_algorithm_gdd(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
        request_id: str,
    ) -> dict[str, Any]:
        source, range_start, range_end = self._algorithm_context(db, user, field_id, query)
        tbase = float(self._query_str(query, "tbase", required=False) or 10.0)

        rows = self._load_metric_rows(
            db,
            field_id=field_id,
            source=source,
            metrics=["temperature"],
            range_start=range_start,
            range_end=range_end,
        )
        rows_by_metric = self._rows_by_metric(rows)
        self._validate_metric_units(rows_by_metric, ["temperature"])

        quality_summary = self._algorithm_quality_summary(rows)
        raw = {
            "source": source,
            "last_sync_at": self._source_last_sync(db, source),
            "data_coverage": self._coverage_percent(range_start, range_end, len(rows), 1),
            "quality_flags": self._quality_flags_summary(rows),
            "values": rows,
        }

        temp_rows = rows_by_metric.get("temperature", [])
        if not temp_rows:
            derived = {
                "status": "INSUFFICIENT_DATA",
                "reason": "Недостаточно данных: отсутствуют температуры",
                "algorithm_id": "gdd",
                "algorithm_version": ALGORITHM_VERSION,
                "inputs_used": {"metrics": ["temperature"], "tbase_c": tbase},
                "quality_summary": quality_summary,
                "values": [],
            }
            self._store_algorithm_run(
                db,
                field_id=field_id,
                source=source,
                algorithm_id="gdd",
                range_start=range_start,
                range_end=range_end,
                status="INSUFFICIENT_DATA",
                reason=derived["reason"],
                error_code=None,
                inputs_used=derived["inputs_used"],
                quality_summary=quality_summary,
                result_payload={"values": []},
                user_id=user.user_id,
                request_id=request_id,
            )
            return {
                "field_id": field_id,
                "from": _iso_utc(range_start),
                "to": _iso_utc(range_end),
                "raw": raw,
                "derived": derived,
            }

        by_day: dict[str, list[float]] = {}
        for row in temp_rows:
            ts = str(row.get("timestamp") or "")
            day = ts[:10]
            by_day.setdefault(day, []).append(float(row.get("value") or 0.0))

        values: list[dict[str, Any]] = []
        gdd_accum = 0.0
        for day in sorted(by_day.keys()):
            tmean = sum(by_day[day]) / max(1, len(by_day[day]))
            gdd_day = max(0.0, tmean - tbase)
            gdd_accum += gdd_day
            values.append(
                {
                    "date": day,
                    "tmean_c": round(tmean, 4),
                    "gdd_day": round(gdd_day, 4),
                    "gdd_accum": round(gdd_accum, 4),
                }
            )

        derived = {
            "status": "OK",
            "reason": None,
            "algorithm_id": "gdd",
            "algorithm_version": ALGORITHM_VERSION,
            "inputs_used": {"metrics": ["temperature"], "tbase_c": tbase},
            "quality_summary": quality_summary,
            "values": values,
        }
        self._store_algorithm_run(
            db,
            field_id=field_id,
            source=source,
            algorithm_id="gdd",
            range_start=range_start,
            range_end=range_end,
            status="OK",
            reason=None,
            error_code=None,
            inputs_used=derived["inputs_used"],
            quality_summary=quality_summary,
            result_payload={"values": values},
            user_id=user.user_id,
            request_id=request_id,
        )
        return {
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "raw": raw,
            "derived": derived,
        }

    def _get_algorithm_vpd(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
        request_id: str,
    ) -> dict[str, Any]:
        source, range_start, range_end = self._algorithm_context(db, user, field_id, query)
        granularity = self._query_str(query, "granularity", required=False) or "hour"
        if granularity not in {"hour", "day"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: granularity", status=422)

        rows = self._load_metric_rows(
            db,
            field_id=field_id,
            source=source,
            metrics=["temperature", "humidity_rh"],
            range_start=range_start,
            range_end=range_end,
        )
        rows_by_metric = self._rows_by_metric(rows)
        self._validate_metric_units(rows_by_metric, ["temperature", "humidity_rh"])
        quality_summary = self._algorithm_quality_summary(rows)

        raw = {
            "source": source,
            "last_sync_at": self._source_last_sync(db, source),
            "data_coverage": self._coverage_percent(range_start, range_end, len(rows), 2),
            "quality_flags": self._quality_flags_summary(rows),
            "values": rows,
        }

        temp_by_ts = {str(row.get("timestamp")): float(row.get("value") or 0.0) for row in rows_by_metric.get("temperature", [])}
        rh_by_ts = {str(row.get("timestamp")): float(row.get("value") or 0.0) for row in rows_by_metric.get("humidity_rh", [])}
        common_ts = sorted(set(temp_by_ts.keys()) & set(rh_by_ts.keys()))

        if not common_ts:
            derived = {
                "status": "INSUFFICIENT_DATA",
                "reason": "Недостаточно данных: нужны температура и влажность",
                "algorithm_id": "vpd",
                "algorithm_version": ALGORITHM_VERSION,
                "inputs_used": {"metrics": ["temperature", "humidity_rh"], "formula": "es-ea"},
                "quality_summary": quality_summary,
                "values": [],
            }
            self._store_algorithm_run(
                db,
                field_id=field_id,
                source=source,
                algorithm_id="vpd",
                range_start=range_start,
                range_end=range_end,
                status="INSUFFICIENT_DATA",
                reason=derived["reason"],
                error_code=None,
                inputs_used=derived["inputs_used"],
                quality_summary=quality_summary,
                result_payload={"values": []},
                user_id=user.user_id,
                request_id=request_id,
            )
            return {
                "field_id": field_id,
                "from": _iso_utc(range_start),
                "to": _iso_utc(range_end),
                "raw": raw,
                "derived": derived,
            }

        vpd_points: list[dict[str, Any]] = []
        for ts in common_ts:
            t = temp_by_ts[ts]
            rh = max(0.0, min(100.0, rh_by_ts[ts]))
            es = 0.6108 * math.exp((17.27 * t) / (t + 237.3))
            ea = es * (rh / 100.0)
            vpd = max(0.0, es - ea)
            vpd_points.append({"timestamp": ts, "temperature_c": round(t, 4), "humidity_rh": round(rh, 4), "vpd_kpa": round(vpd, 6)})

        if granularity == "day":
            by_day: dict[str, list[float]] = {}
            for item in vpd_points:
                by_day.setdefault(str(item["timestamp"])[:10], []).append(float(item["vpd_kpa"]))
            values = [
                {
                    "date": day,
                    "vpd_kpa": round(sum(vals) / max(1, len(vals)), 6),
                    "vpd_min_kpa": round(min(vals), 6),
                    "vpd_max_kpa": round(max(vals), 6),
                }
                for day, vals in sorted(by_day.items())
            ]
        else:
            values = vpd_points

        derived = {
            "status": "OK",
            "reason": None,
            "algorithm_id": "vpd",
            "algorithm_version": ALGORITHM_VERSION,
            "inputs_used": {"metrics": ["temperature", "humidity_rh"], "formula": "es-ea", "granularity": granularity},
            "quality_summary": quality_summary,
            "values": values,
        }
        self._store_algorithm_run(
            db,
            field_id=field_id,
            source=source,
            algorithm_id="vpd",
            range_start=range_start,
            range_end=range_end,
            status="OK",
            reason=None,
            error_code=None,
            inputs_used=derived["inputs_used"],
            quality_summary=quality_summary,
            result_payload={"values": values},
            user_id=user.user_id,
            request_id=request_id,
        )
        return {
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "raw": raw,
            "derived": derived,
        }

    def _get_algorithm_et0(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
        request_id: str,
    ) -> dict[str, Any]:
        source, range_start, range_end = self._algorithm_context(db, user, field_id, query)
        granularity = self._query_str(query, "granularity", required=False) or "day"
        if granularity not in {"hour", "day"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: granularity", status=422)
        allow_approx = self._query_bool(query, "allow_approx", default=True)

        metrics = ["temperature", "humidity_rh", "wind_speed", "radiation"]
        rows = self._load_metric_rows(
            db,
            field_id=field_id,
            source=source,
            metrics=metrics,
            range_start=range_start,
            range_end=range_end,
        )
        rows_by_metric = self._rows_by_metric(rows)
        self._validate_metric_units(rows_by_metric, metrics)
        quality_summary = self._algorithm_quality_summary(rows)

        raw = {
            "source": source,
            "last_sync_at": self._source_last_sync(db, source),
            "data_coverage": self._coverage_percent(range_start, range_end, len(rows), len(metrics)),
            "quality_flags": self._quality_flags_summary(rows),
            "values": rows,
        }

        data_by_ts: dict[str, dict[str, float]] = {}
        for metric in metrics:
            for row in rows_by_metric.get(metric, []):
                ts = str(row.get("timestamp"))
                data_by_ts.setdefault(ts, {})[metric] = float(row.get("value") or 0.0)

        points: list[dict[str, Any]] = []
        variant = "fao56_simplified"
        warnings: list[str] = []
        for ts in sorted(data_by_ts.keys()):
            item = data_by_ts[ts]
            if "temperature" not in item:
                continue

            t = item["temperature"]
            rh = item.get("humidity_rh")
            wind = item.get("wind_speed")
            rad = item.get("radiation")

            if rh is None or wind is None or rad is None:
                if not allow_approx:
                    continue
                variant = "approx"
                rh = 60.0 if rh is None else rh
                wind = 2.0 if wind is None else wind
                rad = 220.0 if rad is None else rad
                warnings = ["Использован упрощённый вариант ET0 (algorithm_variant=approx)"]

            rh = max(0.0, min(100.0, float(rh)))
            wind = max(0.0, float(wind))
            rad = max(0.0, float(rad))

            es = 0.6108 * math.exp((17.27 * t) / (t + 237.3))
            ea = es * (rh / 100.0)
            delta = 4098.0 * es / ((t + 237.3) ** 2)
            gamma = 0.066
            rn = (rad * 0.0864) * 0.77
            numerator = 0.408 * delta * rn + gamma * (900.0 / (t + 273.0)) * wind * max(0.0, es - ea)
            denominator = delta + gamma * (1.0 + 0.34 * wind)
            et0 = max(0.0, numerator / denominator) if denominator > 0 else 0.0

            points.append(
                {
                    "timestamp": ts,
                    "temperature_c": round(t, 4),
                    "humidity_rh": round(rh, 4),
                    "wind_ms": round(wind, 4),
                    "radiation_wm2": round(rad, 4),
                    "et0_mm_day": round(et0, 6),
                }
            )

        if not points:
            reason = "Недостаточно данных: ET0 требует температуру, влажность, ветер и радиацию"
            if allow_approx:
                reason += " (или приблизительный режим)"
            derived = {
                "status": "INSUFFICIENT_DATA",
                "reason": reason,
                "algorithm_id": "et0",
                "algorithm_version": ALGORITHM_VERSION,
                "algorithm_variant": "approx" if allow_approx else "strict",
                "inputs_used": {"metrics": metrics, "granularity": granularity, "allow_approx": allow_approx},
                "quality_summary": quality_summary,
                "warnings": warnings,
                "values": [],
            }
            self._store_algorithm_run(
                db,
                field_id=field_id,
                source=source,
                algorithm_id="et0",
                range_start=range_start,
                range_end=range_end,
                status="INSUFFICIENT_DATA",
                reason=reason,
                error_code=None,
                inputs_used=derived["inputs_used"],
                quality_summary=quality_summary,
                result_payload={"values": []},
                user_id=user.user_id,
                request_id=request_id,
            )
            return {
                "field_id": field_id,
                "from": _iso_utc(range_start),
                "to": _iso_utc(range_end),
                "raw": raw,
                "derived": derived,
            }

        if granularity == "day":
            by_day: dict[str, list[float]] = {}
            for item in points:
                by_day.setdefault(str(item["timestamp"])[:10], []).append(float(item["et0_mm_day"]))
            values = [
                {
                    "date": day,
                    "et0_mm_day": round(sum(vals) / max(1, len(vals)), 6),
                    "et0_min_mm_day": round(min(vals), 6),
                    "et0_max_mm_day": round(max(vals), 6),
                }
                for day, vals in sorted(by_day.items())
            ]
        else:
            values = points

        derived = {
            "status": "OK",
            "reason": None,
            "algorithm_id": "et0",
            "algorithm_version": ALGORITHM_VERSION,
            "algorithm_variant": variant,
            "inputs_used": {"metrics": metrics, "granularity": granularity, "allow_approx": allow_approx},
            "quality_summary": quality_summary,
            "warnings": warnings,
            "values": values,
        }
        self._store_algorithm_run(
            db,
            field_id=field_id,
            source=source,
            algorithm_id="et0",
            range_start=range_start,
            range_end=range_end,
            status="OK",
            reason=None,
            error_code=None,
            inputs_used=derived["inputs_used"],
            quality_summary=quality_summary,
            result_payload={"values": values},
            user_id=user.user_id,
            request_id=request_id,
        )
        return {
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "raw": raw,
            "derived": derived,
        }

    def _get_algorithm_water_deficit(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
        request_id: str,
    ) -> dict[str, Any]:
        source, range_start, range_end = self._algorithm_context(db, user, field_id, query)

        precip = self._metric_aggregate(
            db,
            field_id=field_id,
            source=source,
            metric="precipitation",
            range_start=range_start,
            range_end=range_end,
            agg="sum",
        )
        et0_payload = self._get_algorithm_et0(db, user, field_id, {"from": [_iso_utc(range_start)], "to": [_iso_utc(range_end)], "source": [source], "granularity": ["day"], "allow_approx": ["true"]}, request_id)
        et0_values = et0_payload.get("derived", {}).get("values", []) if isinstance(et0_payload.get("derived"), dict) else []
        et0_sum = 0.0
        for row in et0_values:
            if isinstance(row, dict):
                et0_sum += float(row.get("et0_mm_day") or 0.0)

        status = "OK"
        reason = None
        if precip is None and et0_sum <= 0:
            status = "INSUFFICIENT_DATA"
            reason = "Недостаточно данных: отсутствуют осадки и ET0"

        water_deficit = et0_sum - float(precip or 0.0)
        quality_summary = et0_payload.get("derived", {}).get("quality_summary", {}) if isinstance(et0_payload.get("derived"), dict) else {}
        derived = {
            "status": status,
            "reason": reason,
            "algorithm_id": "water_deficit",
            "algorithm_version": ALGORITHM_VERSION,
            "inputs_used": {"metrics": ["precipitation", "et0"], "formula": "et0_sum - precip_sum"},
            "quality_summary": quality_summary,
            "values": [
                {
                    "from": _iso_utc(range_start),
                    "to": _iso_utc(range_end),
                    "et0_sum_mm": round(et0_sum, 6),
                    "precip_sum_mm": round(float(precip or 0.0), 6),
                    "water_deficit_mm": round(water_deficit, 6),
                }
            ],
        }
        self._store_algorithm_run(
            db,
            field_id=field_id,
            source=source,
            algorithm_id="water_deficit",
            range_start=range_start,
            range_end=range_end,
            status=status,
            reason=reason,
            error_code=None,
            inputs_used=derived["inputs_used"],
            quality_summary=(quality_summary if isinstance(quality_summary, dict) else {}),
            result_payload={"values": derived["values"]},
            user_id=user.user_id,
            request_id=request_id,
        )

        raw_values = self._load_metric_rows(
            db,
            field_id=field_id,
            source=source,
            metrics=["precipitation", "temperature", "humidity_rh", "wind_speed", "radiation"],
            range_start=range_start,
            range_end=range_end,
        )
        raw = {
            "source": source,
            "last_sync_at": self._source_last_sync(db, source),
            "data_coverage": self._coverage_percent(range_start, range_end, len(raw_values), 5),
            "quality_flags": self._quality_flags_summary(raw_values),
            "values": raw_values,
        }
        return {
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "raw": raw,
            "derived": derived,
        }

    # -------------------------------
    # Assistant rules / alerts
    # -------------------------------
    def _list_assistant_rules(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        where_parts = ["1=1"]

        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"r.enterprise_id = {user.enterprise_id}")

        active_filter = self._query_str(query, "active", required=False)
        if active_filter is not None:
            is_active = self._as_bool(active_filter)
            where_parts.append(f"r.is_active = {str(is_active).upper()}")

        field_filter = self._query_int(query, "field_id")
        if field_filter is not None:
            where_parts.append(f"r.field_id = {field_filter}")

        sort_sql = self._sort_clause(
            self._query_str(query, "sort", required=False),
            {"id", "created_at", "updated_at", "period_hours"},
            "id",
        )

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    r.id,
                    r.enterprise_id,
                    r.field_id,
                    r.parameter,
                    r.condition_code,
                    r.threshold_value,
                    r.threshold_min,
                    r.threshold_max,
                    r.period_hours,
                    r.recommendation_text,
                    r.severity,
                    r.is_active,
                    r.created_by,
                    r.updated_by,
                    to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(r.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM assistant_rules r
                WHERE {' AND '.join(where_parts)}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _create_assistant_rule(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        enterprise_id = self._int_optional(payload.get("enterprise_id"), "enterprise_id")
        field_id = self._int_optional(payload.get("field_id"), "field_id")

        if field_id is not None:
            field = self._get_field(db, user, field_id, include_deleted=False)
            enterprise_from_field = int(field["enterprise_id"])
            if enterprise_id is None:
                enterprise_id = enterprise_from_field
            if enterprise_id != enterprise_from_field:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: field/enterprise mismatch", status=422)

        if enterprise_id is None:
            if user.enterprise_id is None:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: enterprise_id обязателен", status=422)
            enterprise_id = user.enterprise_id

        self._assert_enterprise_scope(user, enterprise_id)

        parameter = self._str_value(payload.get("parameter"), "parameter", min_len=4, max_len=20)
        if parameter not in {"wind", "precipitation", "temperature", "frost"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: parameter", status=422)

        condition_code = self._str_value(payload.get("condition"), "condition", min_len=2, max_len=10)
        if condition_code not in {"gt", "lt", "between"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: condition", status=422)

        threshold_value = payload.get("threshold")
        threshold_min = payload.get("threshold_min")
        threshold_max = payload.get("threshold_max")

        threshold_value_sql = (
            str(float(threshold_value)) if threshold_value is not None else "NULL"
        )
        threshold_min_sql = str(float(threshold_min)) if threshold_min is not None else "NULL"
        threshold_max_sql = str(float(threshold_max)) if threshold_max is not None else "NULL"

        if condition_code == "between" and (threshold_min is None or threshold_max is None):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: threshold_min/threshold_max обязательны", status=422)

        if condition_code in {"gt", "lt"} and threshold_value is None:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: threshold обязателен", status=422)

        period_hours = self._int_value(payload.get("period_hours", 24), "period_hours", min_value=1, max_value=720)
        recommendation_text = self._str_value(payload.get("recommendation_text"), "recommendation_text", min_len=3, max_len=1000)
        severity = self._str_value(payload.get("severity", "warn"), "severity", min_len=4, max_len=8)
        if severity not in {"info", "warn", "critical"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: severity", status=422)

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO assistant_rules (
                    enterprise_id,
                    field_id,
                    parameter,
                    condition_code,
                    threshold_value,
                    threshold_min,
                    threshold_max,
                    period_hours,
                    recommendation_text,
                    severity,
                    is_active,
                    created_by,
                    updated_by
                ) VALUES (
                    {enterprise_id},
                    {str(field_id) if field_id is not None else 'NULL'},
                    {_sql_quote(parameter)},
                    {_sql_quote(condition_code)},
                    {threshold_value_sql},
                    {threshold_min_sql},
                    {threshold_max_sql},
                    {period_hours},
                    {_sql_quote(recommendation_text)},
                    {_sql_quote(severity)},
                    TRUE,
                    {user.user_id},
                    {user.user_id}
                )
                RETURNING *
            )
            SELECT row_to_json(ins) FROM ins;
            """
        )
        assert isinstance(created, dict)
        self._write_audit(db, user.user_id, "assistant.rule.create", "assistant_rule", str(created["id"]), None, created, request_id)
        return created

    def _update_assistant_rule(
        self,
        db: DbClient,
        user: UserContext,
        rule_id: int,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        before = self._get_assistant_rule(db, user, rule_id)

        recommendation_text = payload.get("recommendation_text", before.get("recommendation_text"))
        recommendation_text = self._str_value(recommendation_text, "recommendation_text", min_len=3, max_len=1000)

        severity = payload.get("severity", before.get("severity"))
        severity = self._str_value(severity, "severity", min_len=4, max_len=8)
        if severity not in {"info", "warn", "critical"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: severity", status=422)

        is_active = payload.get("is_active", before.get("is_active"))
        is_active_bool = self._bool_value(is_active, "is_active")

        db.exec_checked(
            f"""
            UPDATE assistant_rules
            SET
                recommendation_text = {_sql_quote(recommendation_text)},
                severity = {_sql_quote(severity)},
                is_active = {str(is_active_bool).upper()},
                updated_by = {user.user_id},
                updated_at = NOW()
            WHERE id = {rule_id};
            """
        )

        after = self._get_assistant_rule(db, user, rule_id)
        self._write_audit(db, user.user_id, "assistant.rule.update", "assistant_rule", str(rule_id), before, after, request_id)
        return after

    def _archive_assistant_rule(
        self,
        db: DbClient,
        user: UserContext,
        rule_id: int,
        request_id: str,
    ) -> dict[str, Any]:
        before = self._get_assistant_rule(db, user, rule_id)
        db.exec_checked(
            f"""
            UPDATE assistant_rules
            SET is_active = FALSE, updated_at = NOW(), updated_by = {user.user_id}
            WHERE id = {rule_id};
            """
        )
        after = self._get_assistant_rule(db, user, rule_id)
        self._write_audit(db, user.user_id, "assistant.rule.archive", "assistant_rule", str(rule_id), before, after, request_id)
        return after

    def _get_assistant_rule(self, db: DbClient, user: UserContext, rule_id: int) -> dict[str, Any]:
        where_parts = [f"id = {rule_id}"]
        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"enterprise_id = {user.enterprise_id}")

        payload = db.query_json(
            f"""
            SELECT row_to_json(r)
            FROM (
                SELECT * FROM assistant_rules WHERE {' AND '.join(where_parts)}
            ) r;
            """
        )
        if not isinstance(payload, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)
        return payload

    def _get_assistant_alerts(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        enterprise_id = int(field["enterprise_id"])
        self._assert_enterprise_scope(user, enterprise_id)

        range_start = self._parse_datetime(self._query_str(query, "from", required=True))
        range_end = self._parse_datetime(self._query_str(query, "to", required=True))

        rules = self._load_applicable_rules(db, enterprise_id, field_id)
        alerts: list[dict[str, Any]] = []

        for rule in rules:
            evaluation = self._evaluate_rule(db, rule, field_id, range_start, range_end)
            if not evaluation["triggered"]:
                continue

            alerts.append(
                {
                    "rule_id": rule["id"],
                    "type": rule["parameter"],
                    "level": rule["severity"],
                    "reason": evaluation["reason"],
                    "evidence": evaluation["evidence"],
                    "triggered_at": _iso_utc(datetime.now(timezone.utc)),
                    "recommendation": rule["recommendation_text"],
                }
            )

        return {
            "field_id": field_id,
            "from": _iso_utc(range_start),
            "to": _iso_utc(range_end),
            "alerts": alerts,
        }

    def _get_assistant_recommendations(
        self,
        db: DbClient,
        user: UserContext,
        field_id: int,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        field = self._get_field(db, user, field_id, include_deleted=False)
        enterprise_id = int(field["enterprise_id"])
        self._assert_enterprise_scope(user, enterprise_id)

        at = self._parse_datetime(self._query_str(query, "at", required=True))
        rules = self._load_applicable_rules(db, enterprise_id, field_id)

        recommendations: list[dict[str, Any]] = []
        for rule in rules:
            period_hours = int(rule["period_hours"])
            range_start = at - timedelta(hours=period_hours)
            evaluation = self._evaluate_rule(db, rule, field_id, range_start, at)
            if not evaluation["triggered"]:
                continue

            recommendations.append(
                {
                    "rule_id": rule["id"],
                    "what_to_do": rule["recommendation_text"],
                    "why": evaluation["reason"],
                    "data_quality": evaluation["quality"],
                    "level": rule["severity"],
                    "at": _iso_utc(at),
                    "based_on_data": evaluation["evidence"],
                    "confidence": ("низкое" if evaluation["quality"] == "low" else "высокое"),
                    "alternatives": [
                        "Перенести операцию на окно со скоростью ветра ниже порога",
                        "Уточнить прогноз через 3-6 часов перед выездом в поле",
                    ] if str(rule.get("parameter")) == "wind" else [],
                }
            )

        return {
            "field_id": field_id,
            "at": _iso_utc(at),
            "recommendations": recommendations,
        }

    def _create_assistant_decision(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        field_id = self._int_value(payload.get("field_id"), "field_id", min_value=1)
        field = self._get_field(db, user, field_id, include_deleted=False)
        self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        rule_id = self._int_optional(payload.get("rule_id"), "rule_id")
        decision = self._str_value(payload.get("decision"), "decision", min_len=5, max_len=10)
        if decision not in {"shown", "confirmed", "rejected"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: decision", status=422)

        recommendation_text = payload.get("recommendation_text")
        recommendation_sql = (
            _sql_quote(self._str_value(recommendation_text, "recommendation_text", min_len=2, max_len=1000))
            if recommendation_text
            else "NULL"
        )

        reason_json = payload.get("reason", {})
        if not isinstance(reason_json, dict):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: reason", status=422)

        created = db.query_json(
            f"""
            WITH ins AS (
                INSERT INTO assistant_decision_journal (
                    field_id,
                    rule_id,
                    user_id,
                    decision,
                    recommendation_text,
                    reason,
                    request_id,
                    shown_at,
                    decided_at
                ) VALUES (
                    {field_id},
                    {str(rule_id) if rule_id is not None else 'NULL'},
                    {user.user_id},
                    {_sql_quote(decision)},
                    {recommendation_sql},
                    {_sql_quote(json.dumps(reason_json, ensure_ascii=False))}::jsonb,
                    {_sql_quote(request_id)},
                    NOW(),
                    CASE WHEN {_sql_quote(decision)} = 'shown' THEN NULL ELSE NOW() END
                )
                RETURNING *
            )
            SELECT row_to_json(ins) FROM ins;
            """
        )
        assert isinstance(created, dict)
        self._write_audit(
            db,
            user.user_id,
            "assistant.decision.create",
            "assistant_decision",
            str(created["id"]),
            None,
            created,
            request_id,
        )
        return created

    def _list_assistant_decisions(self, db: DbClient, user: UserContext, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        where_parts = ["1=1"]

        field_id = self._query_int(query, "field_id")
        if field_id is not None:
            field = self._get_field(db, user, field_id, include_deleted=False)
            self._assert_enterprise_scope(user, int(field["enterprise_id"]))
            where_parts.append(f"d.field_id = {field_id}")

        if user.role_code != ROLE_ADMIN and user.enterprise_id is not None:
            where_parts.append(f"f.enterprise_id = {user.enterprise_id}")

        sort_sql = self._sort_clause(
            self._query_str(query, "sort", required=False),
            {"id", "shown_at", "created_at"},
            "shown_at",
        )

        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    d.id,
                    d.field_id,
                    d.rule_id,
                    d.user_id,
                    d.decision,
                    d.recommendation_text,
                    d.reason,
                    d.request_id,
                    to_char(d.shown_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS shown_at,
                    CASE
                        WHEN d.decided_at IS NULL THEN NULL
                        ELSE to_char(d.decided_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS decided_at,
                    to_char(d.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM assistant_decision_journal d
                JOIN fields f ON f.id = d.field_id
                WHERE {' AND '.join(where_parts)}
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY {sort_sql}
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    def _load_applicable_rules(self, db: DbClient, enterprise_id: int, field_id: int) -> list[dict[str, Any]]:
        payload = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)
            FROM (
                SELECT *
                FROM assistant_rules
                WHERE is_active = TRUE
                  AND enterprise_id = {enterprise_id}
                  AND (field_id IS NULL OR field_id = {field_id})
                ORDER BY id
            ) r;
            """
        )
        assert isinstance(payload, list)
        return payload

    def _evaluate_rule(
        self,
        db: DbClient,
        rule: dict[str, Any],
        field_id: int,
        range_start: datetime,
        range_end: datetime,
    ) -> dict[str, Any]:
        metric = self._metric_for_rule(str(rule["parameter"]))

        records = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.observed_at), '[]'::json)
            FROM (
                SELECT
                    metric_code,
                    value,
                    unit,
                    quality_flags,
                    observed_at
                FROM provider_observations
                WHERE field_id = {field_id}
                  AND metric_code = {_sql_quote(metric)}
                  AND observed_at BETWEEN {_sql_quote(_iso_utc(range_start))}::timestamptz
                                      AND {_sql_quote(_iso_utc(range_end))}::timestamptz
            ) r;
            """
        )
        assert isinstance(records, list)

        if not records:
            return {
                "triggered": False,
                "reason": "Недостаточно данных",
                "evidence": {"count": 0},
                "quality": "low",
            }

        values = [float(item.get("value") or 0.0) for item in records]
        unit = str(records[0].get("unit") or "")

        if metric == "precipitation":
            observed = sum(values)
            observed_label = "sum"
        elif metric in {"wind_speed", "temperature"}:
            if str(rule["parameter"]) == "frost":
                observed = min(values)
                observed_label = "min"
            else:
                observed = max(values)
                observed_label = "max"
        else:
            observed = sum(values)
            observed_label = "sum"

        condition_code = str(rule["condition_code"])
        triggered = False
        condition_text = ""

        if condition_code == "gt":
            threshold = float(rule.get("threshold_value") or 0.0)
            triggered = observed > threshold
            condition_text = f"{observed:.4f} {unit} > {threshold:.4f} {unit}"
        elif condition_code == "lt":
            threshold = float(rule.get("threshold_value") or 0.0)
            triggered = observed < threshold
            condition_text = f"{observed:.4f} {unit} < {threshold:.4f} {unit}"
        else:
            min_v = float(rule.get("threshold_min") or 0.0)
            max_v = float(rule.get("threshold_max") or 0.0)
            triggered = min_v <= observed <= max_v
            condition_text = f"{min_v:.4f} {unit} <= {observed:.4f} {unit} <= {max_v:.4f} {unit}"

        quality = "ok"
        for row in records:
            flags = [str(flag) for flag in row.get("quality_flags", [])]
            if any(flag in {"cloudy", "low_confidence", "cloud_mask_interpolated", "low_quality"} for flag in flags):
                quality = "low"
                break

        return {
            "triggered": triggered,
            "reason": f"{rule['parameter']}: {condition_text}",
            "evidence": {
                "metric": metric,
                "aggregation": observed_label,
                "observed": round(observed, 4),
                "unit": unit,
                "points": len(records),
            },
            "quality": quality,
        }

    @staticmethod
    def _metric_for_rule(parameter: str) -> str:
        mapping = {
            "wind": "wind_speed",
            "precipitation": "precipitation",
            "temperature": "temperature",
            "frost": "temperature",
        }
        metric = mapping.get(parameter)
        if not metric:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: parameter", status=422)
        return metric

    # -------------------------------
    # Export jobs + TTL
    # -------------------------------
    def _create_export_job(
        self,
        db: DbClient,
        user: UserContext,
        payload: dict[str, Any],
        request_id: str,
        idempotency_key: str | None,
    ) -> ApiHttpResponse:
        entity = self._str_value(payload.get("entity"), "entity", min_len=5, max_len=20)
        if entity not in {"weather", "satellite", "assistant"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: entity", status=422)

        source = self._normalize_source(
            self._str_value(payload.get("source", "Copernicus"), "source", min_len=4, max_len=20)
        )
        granularity = self._str_value(payload.get("granularity", "day"), "granularity", min_len=3, max_len=10)
        if granularity not in {"month", "day", "hour", "point"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: granularity", status=422)

        export_format = self._str_value(payload.get("format", "json"), "format", min_len=3, max_len=10)
        if export_format not in {"json", "csv"}:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: format", status=422)

        range_start = self._parse_datetime(self._str_value(payload.get("from"), "from", min_len=10, max_len=40))
        range_end = self._parse_datetime(self._str_value(payload.get("to"), "to", min_len=10, max_len=40))
        if range_end < range_start:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: конец диапазона меньше начала", status=422)

        field_ids_raw = payload.get("field_ids")
        if not isinstance(field_ids_raw, list) or not field_ids_raw:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: field_ids", status=422)

        field_ids: list[int] = []
        for raw in field_ids_raw:
            field_id = self._int_value(raw, "field_id", min_value=1)
            field = self._get_field(db, user, field_id, include_deleted=False)
            self._assert_enterprise_scope(user, int(field["enterprise_id"]))
            field_ids.append(field_id)

        endpoint = "/api/v1/export"
        request_hash = self._request_hash(payload)
        if idempotency_key:
            replay = self._idempotency_lookup(db, idempotency_key, endpoint, request_hash)
            if replay is not None:
                return self._success(
                    replay["data"],
                    request_id=request_id,
                    status=int(replay["status_code"]),
                    user_id=user.user_id,
                    meta_extra={"idempotent_replay": True},
                )

        export_id = uuid.uuid4().hex

        db.exec_checked(
            f"""
            INSERT INTO api_export_jobs (
                export_id,
                entity,
                source,
                field_ids,
                range_start,
                range_end,
                granularity,
                export_format,
                status,
                request_meta,
                idempotency_key,
                created_by,
                created_at,
                updated_at,
                expires_at
            ) VALUES (
                {_sql_quote(export_id)},
                {_sql_quote(entity)},
                {_sql_quote(source)},
                {_sql_quote(json.dumps(field_ids))}::jsonb,
                {_sql_quote(_iso_utc(range_start))}::timestamptz,
                {_sql_quote(_iso_utc(range_end))}::timestamptz,
                {_sql_quote(granularity)},
                {_sql_quote(export_format)},
                'pending',
                {_sql_quote(json.dumps({"request_id": request_id}, ensure_ascii=False))}::jsonb,
                {(_sql_quote(idempotency_key) if idempotency_key else 'NULL')},
                {user.user_id},
                NOW(),
                NOW(),
                NOW() + INTERVAL '30 days'
            );
            """
        )

        payload_data = self._load_export_job(db, export_id)
        assert isinstance(payload_data, dict)
        self._write_audit(db, user.user_id, "export.create", "export_job", export_id, None, payload_data, request_id)

        status_code = 202
        envelope = {
            "data": payload_data,
            "meta": {
                "api_version": API_VERSION,
                "request_id": request_id,
            },
        }
        if idempotency_key:
            self._idempotency_store(db, idempotency_key, endpoint, request_hash, envelope, status_code)

        return ApiHttpResponse(status_code=status_code, body=self._json_bytes(envelope), user_id=user.user_id)

    def _get_export_job(self, db: DbClient, user: UserContext, export_id: str) -> dict[str, Any]:
        job = self._load_export_job(db, export_id)
        if not isinstance(job, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)

        field_ids = [int(x) for x in (job.get("field_ids") or [])]
        for field_id in field_ids:
            field = self._get_field(db, user, field_id, include_deleted=True)
            self._assert_enterprise_scope(user, int(field["enterprise_id"]))

        if str(job.get("status")) in {"pending", "running"}:
            self._process_export_job(db, job)
            job = self._load_export_job(db, export_id)
            assert isinstance(job, dict)

        return job

    def _load_export_job(self, db: DbClient, export_id: str) -> dict[str, Any] | None:
        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    export_id,
                    entity,
                    source,
                    field_ids,
                    to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                    to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                    granularity,
                    export_format,
                    status,
                    file_path,
                    error_text,
                    request_meta,
                    idempotency_key,
                    created_by,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
                    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at,
                    CASE
                        WHEN warned_at IS NULL THEN NULL
                        ELSE to_char(warned_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS warned_at,
                    extended_count
                FROM api_export_jobs
                WHERE export_id = {_sql_quote(export_id)}
            ) x;
            """
        )
        if not isinstance(payload, dict):
            return None
        return payload

    def _process_export_job(self, db: DbClient, job: dict[str, Any]) -> None:
        export_id = str(job["export_id"])
        status = str(job.get("status") or "")
        if status not in {"pending", "running"}:
            return

        db.exec_checked(
            f"""
            UPDATE api_export_jobs
            SET status = 'running', error_text = NULL, updated_at = NOW()
            WHERE export_id = {_sql_quote(export_id)};
            """
        )

        try:
            rows = self._collect_export_rows(db, job)
            export_format = str(job.get("export_format") or "json")
            EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

            if export_format == "json":
                file_path = EXPORTS_DIR / f"{export_id}.json"
                file_payload = {
                    "export_id": export_id,
                    "entity": job.get("entity"),
                    "source": job.get("source"),
                    "field_ids": job.get("field_ids"),
                    "from": job.get("range_start"),
                    "to": job.get("range_end"),
                    "granularity": job.get("granularity"),
                    "rows": rows,
                }
                file_path.write_text(json.dumps(file_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            else:
                file_path = EXPORTS_DIR / f"{export_id}.csv"
                with file_path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=sorted({k for row in rows for k in row.keys()}))
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)

            db.exec_checked(
                f"""
                UPDATE api_export_jobs
                SET status = 'done',
                    file_path = {_sql_quote(str(file_path.relative_to(ROOT)))},
                    error_text = NULL,
                    updated_at = NOW()
                WHERE export_id = {_sql_quote(export_id)};
                """
            )
        except Exception as exc:  # noqa: BLE001
            db.exec_checked(
                f"""
                UPDATE api_export_jobs
                SET status = 'failed',
                    error_text = {_sql_quote(str(exc)[:1500])},
                    updated_at = NOW()
                WHERE export_id = {_sql_quote(export_id)};
                """
            )

    def _collect_export_rows(self, db: DbClient, job: dict[str, Any]) -> list[dict[str, Any]]:
        export_id = str(job["export_id"])
        entity = str(job["entity"])
        source = str(job["source"])
        field_ids = [int(x) for x in (job.get("field_ids") or [])]
        from_ts = str(job["range_start"])
        to_ts = str(job["range_end"])

        field_ids_sql = ",".join(str(field_id) for field_id in field_ids) or "-1"

        if entity == "assistant":
            payload = db.query_json(
                f"""
                SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.shown_at DESC), '[]'::json)
                FROM (
                    SELECT
                        id,
                        field_id,
                        rule_id,
                        user_id,
                        decision,
                        recommendation_text,
                        reason,
                        request_id,
                        to_char(shown_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS shown_at
                    FROM assistant_decision_journal
                    WHERE field_id IN ({field_ids_sql})
                      AND shown_at BETWEEN {_sql_quote(from_ts)}::timestamptz
                                      AND {_sql_quote(to_ts)}::timestamptz
                ) x;
                """
            )
            assert isinstance(payload, list)
            return payload

        metric_filter = WEATHER_METRICS if entity == "weather" else (SATELLITE_METRICS | SATELLITE_QUALITY_METRICS)
        metrics_sql = ",".join(_sql_quote(metric) for metric in sorted(metric_filter))
        payload = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.timestamp DESC, x.metric), '[]'::json)
            FROM (
                SELECT
                    field_id,
                    metric_code AS metric,
                    ROUND(value::numeric, 4)::double precision AS value,
                    unit,
                    to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                    source,
                    quality_flags,
                    meta
                FROM provider_observations
                WHERE field_id IN ({field_ids_sql})
                  AND source = {_sql_quote(source)}
                  AND metric_code IN ({metrics_sql})
                  AND observed_at BETWEEN {_sql_quote(from_ts)}::timestamptz
                                      AND {_sql_quote(to_ts)}::timestamptz
            ) x;
            """
        )
        assert isinstance(payload, list)
        return payload

    def _extend_export_job(
        self,
        db: DbClient,
        user: UserContext,
        export_id: str,
        days: int,
        request_id: str,
    ) -> dict[str, Any]:
        before = self._load_export_job(db, export_id)
        if not isinstance(before, dict):
            raise ApiError("NOT_FOUND", "Объект не найден", status=404)

        db.exec_checked(
            f"""
            UPDATE api_export_jobs
            SET expires_at = expires_at + INTERVAL '{int(days)} days',
                warned_at = NULL,
                extended_count = extended_count + 1,
                updated_at = NOW()
            WHERE export_id = {_sql_quote(export_id)};
            """
        )
        after = self._load_export_job(db, export_id)
        assert isinstance(after, dict)
        self._write_audit(db, user.user_id, "export.extend", "export_job", export_id, before, after, request_id)
        return after

    def _download_export_job(
        self,
        db: DbClient,
        user: UserContext,
        export_id: str,
        request_id: str,
    ) -> ApiHttpResponse:
        job = self._get_export_job(db, user, export_id)
        if str(job.get("status")) != "done":
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409, details="Экспорт не готов")

        file_path_raw = job.get("file_path")
        if not isinstance(file_path_raw, str) or not file_path_raw:
            raise ApiError("NOT_FOUND", "Объект не найден", status=404, details="Файл экспорта отсутствует")

        file_path = ROOT / file_path_raw
        if not file_path.exists():
            raise ApiError("NOT_FOUND", "Объект не найден", status=404, details="Файл экспорта не найден")

        content = file_path.read_bytes()
        filename = file_path.name
        content_type = "application/json" if filename.endswith(".json") else "text/csv; charset=utf-8"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-API-Version": API_VERSION,
            "X-Request-ID": request_id,
        }
        return ApiHttpResponse(
            status_code=200,
            body=content,
            content_type=content_type,
            headers=headers,
            user_id=user.user_id,
        )

    # -------------------------------
    # Metrics / audit
    # -------------------------------
    def _build_metrics_overview(self, db: DbClient) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        since = now - timedelta(minutes=5)
        since_iso = _iso_utc(since)

        summary = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    COUNT(*)::int AS total_requests,
                    ROUND(COALESCE(COUNT(*) / 300.0, 0)::numeric, 4) AS rps_5m,
                    ROUND(COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms), 0)::numeric, 2) AS latency_p95_ms,
                    ROUND(
                        CASE WHEN COUNT(*) = 0 THEN 0
                             ELSE (COUNT(*) FILTER (WHERE error_code = 'NO_DATA')::numeric / COUNT(*)::numeric) * 100
                        END,
                        2
                    ) AS no_data_share_percent
                FROM api_request_log
                WHERE created_at >= {_sql_quote(since_iso)}::timestamptz
            ) x;
            """
        )
        assert isinstance(summary, dict)

        errors = db.query_json(
            f"""
            SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
            FROM (
                SELECT COALESCE(error_code, 'NONE') AS error_code, COUNT(*)::int AS total
                FROM api_request_log
                WHERE created_at >= {_sql_quote(since_iso)}::timestamptz
                GROUP BY COALESCE(error_code, 'NONE')
                ORDER BY error_code
            ) x;
            """
        )
        assert isinstance(errors, list)

        sync_stats = db.query_json(
            """
            SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
            FROM (
                SELECT source, status, last_sync_at, last_success_at, last_error
                FROM provider_sync_status
                ORDER BY source
            ) x;
            """
        )
        assert isinstance(sync_stats, list)

        return {
            "window_minutes": 5,
            "summary": summary,
            "errors_by_code": errors,
            "sync_status": sync_stats,
        }

    def _list_audit_log(self, db: DbClient, query: dict[str, list[str]]) -> dict[str, Any]:
        page, page_size, offset = self._pagination(query)
        payload = db.query_json(
            f"""
            WITH base AS (
                SELECT
                    id,
                    user_id,
                    action,
                    object_type,
                    object_id,
                    before_state,
                    after_state,
                    request_id,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
                FROM api_audit_log
            ),
            paged AS (
                SELECT * FROM base
                ORDER BY id DESC
                LIMIT {page_size}
                OFFSET {offset}
            )
            SELECT json_build_object(
                'items', COALESCE((SELECT json_agg(row_to_json(p)) FROM paged p), '[]'::json),
                'page', {page},
                'page_size', {page_size},
                'total', (SELECT COUNT(*)::int FROM base)
            );
            """
        )
        assert isinstance(payload, dict)
        return payload

    # -------------------------------
    # Shared helpers
    # -------------------------------
    def _write_audit(
        self,
        db: DbClient,
        user_id: int | None,
        action: str,
        object_type: str,
        object_id: str | None,
        before_state: Any,
        after_state: Any,
        request_id: str,
    ) -> None:
        db.exec_checked(
            f"""
            INSERT INTO api_audit_log (
                user_id,
                action,
                object_type,
                object_id,
                before_state,
                after_state,
                request_id
            ) VALUES (
                {str(user_id) if user_id is not None else 'NULL'},
                {_sql_quote(action)},
                {_sql_quote(object_type)},
                {(_sql_quote(object_id) if object_id else 'NULL')},
                {(_sql_quote(json.dumps(before_state, ensure_ascii=False)) + '::jsonb') if before_state is not None else 'NULL'},
                {(_sql_quote(json.dumps(after_state, ensure_ascii=False)) + '::jsonb') if after_state is not None else 'NULL'},
                {_sql_quote(request_id)}
            );
            """
        )

    def _idempotency_lookup(
        self,
        db: DbClient,
        key: str,
        endpoint: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        payload = db.query_json(
            f"""
            SELECT row_to_json(x)
            FROM (
                SELECT
                    endpoint,
                    request_hash,
                    response_payload,
                    status_code
                FROM api_idempotency_keys
                WHERE idempotency_key = {_sql_quote(key)}
            ) x;
            """
        )
        if not isinstance(payload, dict):
            return None

        if str(payload.get("endpoint")) != endpoint or str(payload.get("request_hash")) != request_hash:
            raise ApiError(
                "CONFLICT",
                "Конфликт состояния (повторная операция)",
                status=409,
                details="Idempotency-Key уже использован с другим запросом",
            )

        response_payload = payload.get("response_payload")
        if not isinstance(response_payload, dict):
            raise ApiError("CONFLICT", "Конфликт состояния (повторная операция)", status=409)

        return {
            "data": response_payload.get("data"),
            "status_code": int(payload.get("status_code") or 200),
        }

    def _idempotency_store(
        self,
        db: DbClient,
        key: str,
        endpoint: str,
        request_hash: str,
        response_payload: dict[str, Any],
        status_code: int,
    ) -> None:
        db.exec_checked(
            f"""
            INSERT INTO api_idempotency_keys (
                idempotency_key,
                endpoint,
                request_hash,
                response_payload,
                status_code
            ) VALUES (
                {_sql_quote(key)},
                {_sql_quote(endpoint)},
                {_sql_quote(request_hash)},
                {_sql_quote(json.dumps(response_payload, ensure_ascii=False))}::jsonb,
                {int(status_code)}
            )
            ON CONFLICT (idempotency_key) DO UPDATE
            SET endpoint = EXCLUDED.endpoint,
                request_hash = EXCLUDED.request_hash,
                response_payload = EXCLUDED.response_payload,
                status_code = EXCLUDED.status_code;
            """
        )

    @staticmethod
    def _request_hash(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _geometry_sql(self, payload: dict[str, Any]) -> str:
        srid = self._int_value(payload.get("srid", 4326), "srid", min_value=1, max_value=999999)
        if srid != 4326:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: ожидается SRID EPSG:4326", status=422)

        if payload.get("geojson") is not None:
            geojson = payload["geojson"]
            if isinstance(geojson, dict):
                geojson_text = json.dumps(geojson, ensure_ascii=False)
            else:
                raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: geojson", status=422)
            return f"ST_SetSRID(ST_GeomFromGeoJSON($${geojson_text}$$), 4326)"

        if payload.get("geometry") is not None:
            geometry = payload["geometry"]
            if isinstance(geometry, dict):
                geojson_text = json.dumps(geometry, ensure_ascii=False)
                return f"ST_SetSRID(ST_GeomFromGeoJSON($${geojson_text}$$), 4326)"
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: geometry", status=422)

        if payload.get("wkt") is not None:
            wkt = self._str_value(payload["wkt"], "wkt", min_len=10, max_len=40000)
            return f"ST_SetSRID(ST_GeomFromText({_sql_quote(wkt)}), 4326)"

        raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: требуется geojson или wkt", status=422)

    def _optional_point_sql(self, payload: dict[str, Any]) -> str:
        point = payload.get("point")
        if point is None:
            return "NULL"
        if isinstance(point, dict):
            point_text = json.dumps(point, ensure_ascii=False)
            return f"ST_SetSRID(ST_GeomFromGeoJSON($${point_text}$$), 4326)"
        if isinstance(point, str):
            return f"ST_SetSRID(ST_GeomFromText({_sql_quote(point)}), 4326)"
        raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: point", status=422)

    def _optional_zone_sql(self, payload: dict[str, Any]) -> str:
        zone = payload.get("zone")
        if zone is None:
            return "NULL"
        if isinstance(zone, dict):
            zone_text = json.dumps(zone, ensure_ascii=False)
            return f"ST_SetSRID(ST_GeomFromGeoJSON($${zone_text}$$), 4326)"
        if isinstance(zone, str):
            return f"ST_SetSRID(ST_GeomFromText({_sql_quote(zone)}), 4326)"
        raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: zone", status=422)

    @staticmethod
    def _coverage_percent(start: datetime, end: datetime, actual_points: int, metric_count: int) -> float:
        hours = int((end - start).total_seconds() / 3600) + 1
        expected = max(1, hours * max(1, metric_count))
        return round(min(100.0, (actual_points / expected) * 100.0), 2)

    @staticmethod
    def _map_geometry_error(exc: Stage3Error) -> ApiError:
        text = str(exc)
        if "Полигон самопересекается" in text:
            return ApiError("VALIDATION_ERROR", "Некорректные входные данные: Полигон самопересекается", status=422)
        if "Неверная система координат" in text:
            return ApiError(
                "VALIDATION_ERROR",
                "Некорректные входные данные: Неверная система координат: ожидается EPSG:4326",
                status=422,
            )
        if "Площадь поля должна быть больше 0" in text:
            return ApiError("VALIDATION_ERROR", "Некорректные входные данные: Площадь поля должна быть больше 0", status=422)
        return ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {text}", status=422)

    def _assert_enterprise_scope(self, user: UserContext, enterprise_id: int) -> None:
        if user.role_code == ROLE_ADMIN:
            return
        if user.enterprise_id != enterprise_id:
            raise ApiError("FORBIDDEN", "Недостаточно прав", status=403)

    @staticmethod
    def _header(headers: dict[str, str], name: str) -> str | None:
        return headers.get(name.lower())

    @staticmethod
    def _match_id(path: str, pattern: str) -> int | None:
        match = re.match(pattern, path)
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _match_text(path: str, pattern: str) -> str | None:
        match = re.match(pattern, path)
        if not match:
            return None
        return str(match.group(1))

    def _parse_json_body(self, raw_body: bytes) -> dict[str, Any]:
        if not raw_body:
            return {}
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: JSON", status=422) from exc
        if not isinstance(payload, dict):
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: ожидается объект", status=422)
        return payload

    @staticmethod
    def _json_bytes(payload: Any) -> bytes:
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _success(
        self,
        data: Any,
        *,
        request_id: str,
        status: int = 200,
        user_id: int | None = None,
        meta_extra: dict[str, Any] | None = None,
        error_code: str | None = None,
    ) -> ApiHttpResponse:
        meta = {
            "api_version": API_VERSION,
            "request_id": request_id,
        }
        if meta_extra:
            meta.update(meta_extra)
        envelope = {"data": data, "meta": meta}
        return ApiHttpResponse(
            status_code=status,
            body=self._json_bytes(envelope),
            user_id=user_id,
            error_code=error_code,
        )

    def error_response(self, err: ApiError, request_id: str, user_id: int | None = None) -> ApiHttpResponse:
        payload = {
            "api_version": API_VERSION,
            "request_id": request_id,
            "error": {
                "code": err.code,
                "message": err.message,
                "details": err.details,
            },
        }
        return ApiHttpResponse(
            status_code=err.status,
            body=self._json_bytes(payload),
            error_code=err.code,
            user_id=user_id,
        )

    def internal_error_response(self, request_id: str) -> ApiHttpResponse:
        payload = {
            "api_version": API_VERSION,
            "request_id": request_id,
            "error": {
                "code": "SOURCE_UNAVAILABLE",
                "message": "Источник данных недоступен: внутренняя ошибка сервера",
                "details": None,
            },
        }
        return ApiHttpResponse(
            status_code=500,
            body=self._json_bytes(payload),
            error_code="SOURCE_UNAVAILABLE",
        )

    @staticmethod
    def _no_data_meta(is_no_data: bool, reason: str) -> dict[str, Any]:
        if not is_no_data:
            return {"status": "OK"}
        return {"status": "NO_DATA", "reason": reason}

    def _query_str(self, query: dict[str, list[str]], name: str, *, required: bool) -> str | None:
        values = query.get(name)
        if not values or values[0] == "":
            if required:
                raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: параметр {name} обязателен", status=422)
            return None
        return values[0]

    def _query_int(self, query: dict[str, list[str]], name: str) -> int | None:
        value = self._query_str(query, name, required=False)
        if value is None:
            return None
        return self._int_value(value, name, min_value=1)

    def _query_bool(self, query: dict[str, list[str]], name: str, *, default: bool) -> bool:
        value = self._query_str(query, name, required=False)
        if value is None:
            return default
        return self._as_bool(value)

    def _pagination(self, query: dict[str, list[str]]) -> tuple[int, int, int]:
        page = self._int_optional(self._query_str(query, "page", required=False), "page") or 1
        page_size = self._int_optional(self._query_str(query, "page_size", required=False), "page_size") or 20
        if page < 1 or page > 100000:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: page", status=422)
        if page_size < 1 or page_size > 200:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: page_size", status=422)
        return page, page_size, (page - 1) * page_size

    @staticmethod
    def _sort_clause(sort_raw: str | None, allowed_fields: set[str], default_field: str) -> str:
        if not sort_raw:
            return f"{default_field} DESC"
        field = sort_raw
        direction = "ASC"
        if sort_raw.startswith("-"):
            field = sort_raw[1:]
            direction = "DESC"
        if field not in allowed_fields:
            return f"{default_field} DESC"
        return f"{field} {direction}"

    def _normalize_source(self, raw_source: str) -> str:
        normalized = raw_source.strip()
        if normalized in {"Copernicus", "NASA", "Mock"}:
            return normalized
        mapped = SOURCE_ALIASES.get(normalized.lower())
        if mapped is None:
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: source", status=422)
        return mapped

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        try:
            return _parse_ts(value)
        except Exception as exc:  # noqa: BLE001
            raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: timestamp", status=422) from exc

    def _iso_required(self, value: Any, field_name: str) -> str:
        if value is None:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)
        return _iso_utc(self._parse_datetime(str(value)))

    def _date_value(self, value: Any, field_name: str) -> str:
        text = self._str_value(value, field_name, min_len=10, max_len=10)
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError as exc:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422) from exc
        return text

    def _date_optional(self, value: Any, field_name: str) -> str | None:
        if value is None or value == "":
            return None
        return self._date_value(value, field_name)

    @staticmethod
    def _str_value(value: Any, field_name: str, *, min_len: int, max_len: int) -> str:
        if value is None:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)
        text = str(value).strip()
        if len(text) < min_len or len(text) > max_len:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)
        return text

    @staticmethod
    def _int_value(value: Any, field_name: str, *, min_value: int, max_value: int | None = None) -> int:
        try:
            parsed = int(str(value))
        except Exception as exc:  # noqa: BLE001
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422) from exc
        if parsed < min_value:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)
        if max_value is not None and parsed > max_value:
            raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)
        return parsed

    def _int_optional(self, value: Any, field_name: str) -> int | None:
        if value is None or value == "":
            return None
        return self._int_value(value, field_name, min_value=1)

    def _scalar_int(self, raw: str) -> int:
        for line in str(raw).splitlines():
            text = line.strip()
            if not text:
                continue
            if text.isdigit():
                return int(text)
            try:
                return int(float(text))
            except ValueError:
                continue
        raise ApiError("SOURCE_UNAVAILABLE", "Источник данных недоступен: не удалось прочитать число из БД", status=503)

    def _bool_value(self, value: Any, field_name: str) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            if value in {0, 1}:
                return bool(value)
        if isinstance(value, str):
            return self._as_bool(value)
        raise ApiError("VALIDATION_ERROR", f"Некорректные входные данные: {field_name}", status=422)

    @staticmethod
    def _as_bool(text: str) -> bool:
        normalized = text.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        raise ApiError("VALIDATION_ERROR", "Некорректные входные данные: bool", status=422)


class Stage5RequestHandler(BaseHTTPRequestHandler):
    app: Stage5ApiApp
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def do_PUT(self) -> None:  # noqa: N802
        self._handle()

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle()

    def do_PATCH(self) -> None:  # noqa: N802
        self._handle()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def _handle(self) -> None:
        started = time.perf_counter()
        request_id = self.headers.get("X-Request-ID") or f"req-{uuid.uuid4().hex[:12]}"

        split = urlsplit(self.path)
        query = parse_qs(split.query, keep_blank_values=True)
        body_len = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(body_len) if body_len > 0 else b""
        headers = {k.lower(): v for k, v in self.headers.items()}

        user_id: int | None = None
        error_code: str | None = None

        try:
            response = self.app.handle_request(
                method=self.command,
                path=split.path,
                query=query,
                headers=headers,
                raw_body=raw_body,
                request_id=request_id,
            )
            user_id = response.user_id
            error_code = response.error_code
        except ApiError as err:
            response = self.app.error_response(err, request_id=request_id, user_id=user_id)
            error_code = err.code
        except Exception:  # noqa: BLE001
            response = self.app.internal_error_response(request_id=request_id)
            error_code = "SOURCE_UNAVAILABLE"

        duration_ms = int((time.perf_counter() - started) * 1000)
        try:
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.send_header("Connection", "close")
            if "X-API-Version" not in response.headers:
                self.send_header("X-API-Version", API_VERSION)
            if "X-Request-ID" not in response.headers:
                self.send_header("X-Request-ID", request_id)
            for key, value in response.headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(response.body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            # Клиент мог закрыть соединение до завершения отправки.
            pass
        finally:
            self.app.record_request(
                request_id=request_id,
                user_id=user_id,
                method=self.command,
                endpoint=split.path,
                status_code=response.status_code,
                duration_ms=duration_ms,
                error_code=error_code,
            )


def create_server(config: AppConfig, host: str = "0.0.0.0", port: int = 8000) -> ThreadingHTTPServer:
    app = Stage5ApiApp(config)

    class Handler(Stage5RequestHandler):
        pass

    Handler.app = app
    return ThreadingHTTPServer((host, port), Handler)


def process_pending_exports() -> dict[str, Any]:
    db = DbClient()
    db.ensure_ready()
    app = Stage5ApiApp(AppConfig())
    app._ensure_stage5_seed(db)

    rows = db.query_json(
        """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT
                export_id,
                entity,
                source,
                field_ids,
                to_char(range_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_start,
                to_char(range_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS range_end,
                granularity,
                export_format,
                status
            FROM api_export_jobs
            WHERE status IN ('pending', 'running')
            ORDER BY created_at ASC
        ) x;
        """
    )
    assert isinstance(rows, list)

    processed: list[str] = []
    for row in rows:
        app._process_export_job(db, row)
        processed.append(str(row.get("export_id")))

    return {"processed_count": len(processed), "export_ids": processed}


def run_export_ttl_check() -> dict[str, Any]:
    db = DbClient()
    db.ensure_ready()
    warned_ids: list[str] = []
    rows = db.query_json(
        """
        SELECT COALESCE(json_agg(row_to_json(x)), '[]'::json)
        FROM (
            SELECT export_id
            FROM api_export_jobs
            WHERE expires_at <= (NOW() + INTERVAL '1 day')
              AND warned_at IS NULL
            ORDER BY expires_at ASC
        ) x;
        """
    )
    assert isinstance(rows, list)

    for row in rows:
        export_id = str(row.get("export_id"))
        db.exec_checked(
            f"""
            UPDATE api_export_jobs
            SET warned_at = NOW(),
                updated_at = NOW()
            WHERE export_id = {_sql_quote(export_id)};
            """
        )
        warned_ids.append(export_id)

    return {"warned_count": len(warned_ids), "export_ids": warned_ids}
