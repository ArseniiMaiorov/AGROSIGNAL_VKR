# Devlog

## 2026-02-21 — Этап 1: Инициализация

Сделано:
- Создан каркас проекта (backend/frontend/docs).
- Подготовлен Python backend с endpoint `/health`.
- Добавлены Dockerfile и docker-compose (api, postgis, redis).
- Добавлены команды `make up`, `make lint`, `make test`.
- Добавлены unit-тесты и раннер покрытия 100% для кода в `backend/src`.

Техническое решение по инструментам:
- Для этапа 1 выбран offline-совместимый набор на стандартной библиотеке Python.
- Линт на этапе 1: проверка синтаксиса `compileall`.
- Тесты и покрытие: `unittest` + модуль `trace`.

Уточнение от заказчика:
- В следующих этапах требуется автоматическая смена порта при занятости целевого порта.

## 2026-02-21 — Этап 2: Модель данных и миграции PostGIS

Сделано:
- Реализованы миграции для сущностей: предприятие, поле, культура, сезон, пользователь/роль, дневник работ.
- Добавлена геометрия полей в PostGIS и вычисляемое поле площади `area_ha`.
- Реализован триггер валидации геометрии (тип, SRID, невалидность, самопересечение).
- Добавлены скрипт миграций и интеграционные тесты геофикстур.
- Добавлен автопоиск свободного порта в `make up`.

Проверка:
- `make migrate` — миграции применяются успешно.
- `make test-stage2` — все сценарии PASS, формируется протокол в `backend/reports/tests`.

## 2026-02-21 — Этап 3: Контракт данных, провайдеры, диапазоны, экспорт, TTL

Сделано:
- Введён единый контракт данных для источников Copernicus/NASA/Mock.
- Добавлены таблицы хранения наблюдений, статуса синхронизаций и dataset-экспортов.
- Реализованы backend-режимы:
  - синхронизация источников;
  - запросы диапазонов с гранулярностью month/day/hour/point (drill-down);
  - асинхронный экспорт dataset (json/csv);
  - TTL-предупреждения за 1 день и продление хранения dataset.
- Добавлен циклический режим `stage3-cycle` для автоматического запуска sync/export/ttl.

Проверка:
- `make test-stage3` — PASS: sync, drill-down, export, ttl.
- Формируется протокол: `backend/reports/tests/<дата>_stage3_workflows.md`.

## 2026-02-21 — Этап 4: Proxy-контур загрузчиков, health-check, ретраи, наблюдаемость

Сделано:
- Введена настройка proxy в БД (`proxy_settings`) с RBAC-ограничением только для роли `admin`.
- Формализована область действия proxy: только модули `providers/copernicus/*`, `providers/nasa/*`, `datasets/download/*`.
- Добавлены режимы маршрутизации:
  - `global` (общий proxy);
  - `per_provider` (Copernicus/NASA по отдельности);
  - `bypass_hosts` + политика `direct/force_proxy`.
- Реализован health-check:
  - проверка доступности proxy (TCP + TLS handshake для https endpoint);
  - проверка доступности источника лёгким запросом;
  - запись `last_check_*` и `source_reachability`.
- Реализована политика ретраев:
  - retry для `timeout`, `429`, `502`, `503`, `504`;
  - без retry для `401/403` и остальных `4xx`;
  - backoff + jitter, ограничение количества попыток.
- Добавлены структурные журналы и метрики:
  - `proxy_request_logs` (request_id/provider/proxy_used/http_status/duration/error_class/retry_count и т.д.);
  - агрегация метрик по провайдерам и классам ошибок.
- Добавлен режим деградации:
  - при ошибке источник помечается недоступным;
  - сохраняются причина и данные последней успешной синхронизации.

Проверка:
- `make test-stage4` — PASS: 401/auth, DNS, TLS, 429+retry, per-provider, bypass, логирование `proxy_used`, health-check.
- Формируется протокол: `backend/reports/tests/<дата>_stage4_proxy.md`.

## 2026-02-21 — Доводка этапов 1-4 до полного соответствия

Сделано:
- Добавлена миграция `006_stage3_extend_satellite_metrics.sql`:
  - расширен контракт метрик наблюдений (`ndre`, `ndmi`, `cloud_mask`).
- Добавлен фоновый планировщик `scripts/stage_scheduler.py`:
  - автоматический цикл `sync/export/ttl` с параметрами интервала через env.
- Добавлен единый quality-gate `scripts/run_quality_gate.py` и команда `make quality`.
- Усилен RBAC в stage4 CLI:
  - чтение proxy-настроек, метрик и request-log доступно только `admin`.
- Добавлена санитизация ошибок stage4 для исключения утечки секретов в статусах/логах.
- Для нестабильной локальной Docker-сети добавлен fallback:
  - `make up` проверяет доступность API через published port;
  - при недоступности автоматически переключает API в `local` режим (host process).

Проверка:
- `make up` + `make print-port` показывают `API_MODE` (`container`/`local`).
- `curl http://127.0.0.1:<API_PORT>/health` проходит в `local` режиме.
- `make quality` проходит без ошибок.
