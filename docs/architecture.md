# Архитектура (этапы 1-4)

- `backend`: Python API + режимы обработки данных (sync/query/export/TTL/proxy), миграции PostGIS, тестирование.
- `frontend`: каркас каталогов SPA (заполнение на следующих этапах).
- `docs`: общая проектная документация и протоколы.

Текущий runtime-стек backend:
- API: Python HTTP-сервис (`/health`)
- Сервисные режимы:
  - `stage3_cli.py` (`sync/query/export/ttl`) и циклический запуск `stage3-cycle`;
  - `stage4_cli.py` (`proxy-set/request/health-check/metrics/degradation`).
- БД: PostgreSQL + PostGIS
- Очереди/кэш: Redis
- Оркестрация: Docker Compose

Нефункциональные требования:
- При занятом целевом порте система автоматически выбирает свободный порт и сохраняет его в `backend/.api_port`.

## Этап 2 (данные поля)
- `fields.geom` хранится как `Polygon` в `EPSG:4326`.
- Валидация: запрет самопересечений, пустых/невалидных геометрий.

## Этап 3 (контракт, провайдеры, диапазоны)
- Единый контракт данных для всех источников:
  - `value`, `unit`, `timestamp`, `source`, `quality_flags`, `meta`.
- Провайдеры: `Copernicus`, `NASA`, `Mock`.
- Синхронизация:
  - хранится статус по источнику (`last_sync_at`, `last_success_at`, `status`, `last_error`).
- Drill-down:
  - диапазоны с гранулярностью `month/day/hour/point`;
  - bins и статистика от нового к старому (`desc`).
- Экспорт:
  - асинхронная задача на dataset (`queued/processing/ready/failed`), файл `json/csv`.
- TTL:
  - срок хранения dataset по умолчанию 30 дней;
  - предупреждение за 1 день до истечения;
  - продление срока хранения пользователем.

## Этап 4 (proxy и наблюдаемость)
- Область действия proxy ограничена только outbound-запросами модулей:
  - `providers/copernicus/*`
  - `providers/nasa/*`
  - `datasets/download/*`
- Не затрагиваются:
  - входящие запросы UI/API пользователя;
  - внутренние сервисы (PostgreSQL/Redis);
  - прочие интеграции вне списка модулей загрузки.
- Режимы proxy:
  - `global`: общий proxy для загрузчиков;
  - `per_provider`: отдельное решение для Copernicus/NASA;
  - `bypass_hosts` + `bypass_policy` (`direct`/`force_proxy`).
- Безопасность:
  - секреты proxy не хранятся в БД (используются env/secret-store);
  - операции настройки требуют роль `admin` (RBAC).
- Health-check:
  - proxy: TCP connect + TLS handshake (для `https` proxy endpoint);
  - source: лёгкий GET-запрос к endpoint источника;
  - фиксируются `proxy_last_check_at`, `proxy_check_result`, `source_reachability`.
- Ошибки и ретраи:
  - retry: `timeout`, `429`, `502`, `503`, `504`;
  - no-retry: `401/403` и остальные `4xx` (кроме `429`);
  - backoff: экспоненциальный график (по умолчанию `1,5,15` + jitter).
- Наблюдаемость:
  - структурный журнал запросов: `request_id`, `provider`, `proxy_used`, `target_host`, `http_status`,
    `bytes_downloaded`, `duration_ms`, `error_class`, `retry_count`, `success`;
  - сводные метрики по провайдерам и классам ошибок.
- Деградация:
  - при недоступности источника система сохраняет статус ошибки и последнюю успешную синхронизацию;
  - UI может отображать последнюю успешную дату и причину сбоя без “падения” интерфейса.
