# Changelog

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
