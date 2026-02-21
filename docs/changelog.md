# Changelog

## [0.5.0] - 2026-02-21
### Добавлено
- Этап 5: backend API `/api/v1` без фронта для основных пользовательских сценариев.
- Единый формат ответов/ошибок (`data/meta`, `error.code/message/details`) с `api_version` и `request_id`.
- Доменные endpoint'ы:
  - `enterprises`, `users`, `fields` (soft delete/restore/history), `crops`, `seasons`, `field operations`.
- Endpoint'ы данных:
  - `weather`, `weather/summary`, `satellite/index`, `satellite/scenes`, `satellite/quality`, `sync/status`, `sync/run`.
- Endpoint'ы помощника агронома:
  - `assistant/rules`, `assistant/alerts`, `assistant/recommendations`, `assistant/decisions`.
- Endpoint'ы экспорта:
  - `POST /api/v1/export`, `GET /api/v1/export/{id}`, `GET /api/v1/export/{id}/download`, `POST /api/v1/export/{id}/extend`.
- Идемпотентность для операций `sync/run` и `export` через `Idempotency-Key`.
- Аудит и наблюдаемость:
  - таблицы `api_audit_log`, `api_request_log`, `api_idempotency_keys`, `api_export_jobs`;
  - endpoint `GET /api/v1/metrics/overview`, `GET /api/v1/audit`.
- Новый тестовый набор `make test-stage5` + протокол `backend/reports/tests/*_stage5_api.md`.

### Изменено
- API backend переведён с минимального `/health`-обработчика на полноценный HTTP-router stage5.
- `make quality` расширен проверками этапа 5.
- Планировщик дополняет цикл обработкой stage5 export jobs и TTL-предупреждений.
- Docker image API дополнен `postgresql-client` для стабильной работы SQL-слоя API в container-mode.
- Добавлены миграции:
  - `007_stage5_api_and_user_scenarios.sql`;
  - `008_stage5_fix_field_trigger_order.sql`;
  - `009_stage5_fix_bbox_srid_guard.sql`.

## [0.4.1] - 2026-02-21
### Изменено
- Этап 3: расширен набор метрик контракта (`NDRE`, `NDMI`, `cloud_mask`) и добавлена миграция `006_stage3_extend_satellite_metrics.sql`.
- Добавлен фоновый планировщик `stage_scheduler.py` (автоматический цикл sync/export/ttl по расписанию).
- Добавлен единый quality-gate `make quality` (unit + миграции + stage2/stage3/stage4).
- Этап 4: усилен RBAC для чтения proxy-настроек/метрик/журналов (`admin`).
- Этап 4: добавлена санитизация ошибок для исключения утечки секретов в status/log.
- Добавлен wrapper `stage4_cli.py` в корне репозитория и make-команды для stage4 CLI.
- `make up` получил автоматический fallback в локальный API-режим при недоступном Docker published port.

## [0.4.0] - 2026-02-21
### Добавлено
- Этап 4: proxy-контур загрузчиков датасетов Copernicus/NASA/Mock.
- Таблицы `proxy_settings`, `provider_sync_journal`, `proxy_request_logs`.
- Безопасная схема хранения proxy-секретов: креды читаются из env, в БД хранятся только флаги и endpoint без секретов.
- Режимы `global`/`per_provider`, поддержка `bypass_hosts` и `bypass_policy`.
- Health-check proxy/source с записью `last_check_*` и `source_reachability`.
- Политика ретраев и классификация ошибок (`timeout/dns/tls/auth/http/network`).
- Наблюдаемость: структурные логи запросов и агрегированные метрики.
- Режим деградации для UI при недоступности источника.
- Команды `stage4_cli.py` и тестовый набор `make test-stage4`.

## [0.3.0] - 2026-02-21
### Добавлено
- Этап 3: единый контракт данных и режимы провайдеров Copernicus/NASA/Mock.
- Таблицы `provider_sync_status`, `provider_observations`, `dataset_slices`, `dataset_notifications`.
- Режимы backend для синхронизации, drill-down запросов, экспорта и TTL.
- Циклический режим `stage3-cycle` для автоматизируемого запуска `sync/export/ttl`.
- Интеграционные тесты этапа 3 и протокол `backend/reports/tests/*_stage3_workflows.md`.

## [0.2.0] - 2026-02-21
### Добавлено
- Этап 2: миграции PostGIS для ключевых сущностей домена.
- SQL-валидация геометрии полей (Polygon, SRID=4326, запрет самопересечений).
- Команды `make migrate` и `make test-stage2`.
- Интеграционные фикстуры геометрии и автогенерация протокола теста.
- Автовыбор свободного порта API в `make up`.

## [0.1.0] - 2026-02-21
### Добавлено
- Этап 1: стартовый каркас монорепозитория.
- Базовый Python backend и endpoint `/health`.
- Docker Compose для API, PostGIS и Redis.
- Команды Make для запуска, линтинга и тестов.
- Набор unit-тестов с покрытием 100% текущего backend-кода.
