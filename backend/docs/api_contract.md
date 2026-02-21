# API/режимы backend (этапы 1-5)

## HTTP endpoints

### GET /health
Назначение:
- проверка доступности API и базовой конфигурации окружения.

Успешный ответ `200 OK`:
```json
{
  "status": "ok",
  "service": "zemledar-api",
  "environment": "dev",
  "timestamp": "2026-02-21T12:00:00+00:00"
}
```

### API v1
Бизнес-endpoint'ы доступны по префиксу `/api/v1/...`.

Единый формат успеха:
```json
{
  "data": {},
  "meta": {
    "api_version": "v1",
    "request_id": "req-..."
  }
}
```

Единый формат ошибки:
```json
{
  "api_version": "v1",
  "request_id": "req-...",
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Некорректные входные данные: ...",
    "details": null
  }
}
```

Поддерживаемые коды ошибок:
- `VALIDATION_ERROR`
- `FORBIDDEN`
- `NOT_FOUND`
- `SOURCE_UNAVAILABLE`
- `NO_DATA` (через `meta.status=NO_DATA` при `200`)
- `CONFLICT`

## Этап 5: API сценариев (без фронта)

### Auth
- `GET /api/v1/auth/me`

### Enterprises / Users
- `GET /api/v1/enterprises`
- `POST /api/v1/enterprises`
- `GET /api/v1/enterprises/{id}`
- `PUT /api/v1/enterprises/{id}`
- `POST /api/v1/enterprises/{id}/users/bind`
- `GET /api/v1/users`
- `POST /api/v1/users`

### Fields (геометрия, soft delete, history)
- `GET /api/v1/fields`
- `POST /api/v1/fields`
- `GET /api/v1/fields/{id}`
- `PUT /api/v1/fields/{id}`
- `DELETE /api/v1/fields/{id}` (soft delete)
- `POST /api/v1/fields/{id}/restore`
- `GET /api/v1/fields/{id}/history`

### Crops / Seasons
- `GET /api/v1/crops`
- `POST /api/v1/crops`
- `GET /api/v1/crops/{id}`
- `PUT /api/v1/crops/{id}`
- `GET /api/v1/seasons`
- `POST /api/v1/seasons`
- `GET /api/v1/seasons/{id}`
- `PUT /api/v1/seasons/{id}`

### Operations / Notes
- `GET /api/v1/fields/{id}/operations`
- `POST /api/v1/fields/{id}/operations`

### Weather / Satellite / Sync
- `GET /api/v1/fields/{id}/weather?from=&to=&granularity=month|day|hour&source=`
- `GET /api/v1/fields/{id}/weather/summary?from=&to=&source=`
- `GET /api/v1/fields/{id}/satellite/index?type=ndvi|ndre|ndmi&from=&to=&source=`
- `GET /api/v1/fields/{id}/satellite/scenes?from=&to=&source=`
- `GET /api/v1/fields/{id}/satellite/quality?from=&to=&source=`
- `GET /api/v1/sync/status?source=copernicus|nasa|mock`
- `POST /api/v1/sync/run`

Для пустого диапазона данных применяется контракт:
- HTTP `200 OK`
- `meta.status = "NO_DATA"`
- пустые массивы в `data`.

### Assistant
- `GET /api/v1/assistant/rules`
- `POST /api/v1/assistant/rules`
- `PUT /api/v1/assistant/rules/{id}`
- `DELETE /api/v1/assistant/rules/{id}`
- `GET /api/v1/fields/{id}/assistant/alerts?from=&to=`
- `GET /api/v1/fields/{id}/assistant/recommendations?at=`
- `GET /api/v1/assistant/decisions`
- `POST /api/v1/assistant/decisions`

### Export
- `POST /api/v1/export`
- `GET /api/v1/export/{id}`
- `GET /api/v1/export/{id}/download`
- `POST /api/v1/export/{id}/extend`

`POST /api/v1/export` поддерживает `Idempotency-Key`.

### Observability / Audit
- `GET /api/v1/metrics/overview`
- `GET /api/v1/audit`

## Пагинация, фильтры, сортировка

Списочные endpoint'ы поддерживают:
- `page`
- `page_size`
- `sort` (`-created_at`, `-operation_at` и т.д.)
- `filter`/профильные фильтры (`status`, `field_id`, `enterprise_id`, `active`, `role`).

## Идемпотентность

Для повторяемых операций:
- `POST /api/v1/sync/run`
- `POST /api/v1/export`

поддерживается заголовок `Idempotency-Key`.

Повтор запроса с тем же ключом и тем же телом возвращает тот же результат.
Если тело запроса другое — возвращается `409 CONFLICT`.

## Режимы backend (этап 3: CLI)

Режимы реализованы через `scripts/stage3_cli.py` и используются для системных циклов/регрессионных проверок.

### Синхронизация
- `sync --source <Copernicus|NASA|Mock> --hours <N> [--field-id <id>]`
- `sync-status --source <Copernicus|NASA|Mock>`

### Drill-down и диапазоны
- `query --source <source> --field-id <id> --from <ISO> --to <ISO> --granularity <month|day|hour|point>`

### Экспорт / TTL
- `export-create --source <source> --field-id <id> --from <ISO> --to <ISO> --granularity <g> --format <json|csv>`
- `export-process [--dataset-id <id>]`
- `export-status --dataset-id <id>`
- `ttl-check`
- `dataset-extend --dataset-id <id> --days <N>`
- `dataset-view --dataset-id <id> [--granularity <g>]`

## Режимы backend (этап 4: proxy CLI)

Режимы реализованы через `scripts/stage4_cli.py`.

### Admin/RBAC
- `ensure-admin --email <admin@email>`
- `proxy-set`/`proxy-get`/`metrics`/`request-log` доступны только для `admin`.

### Конфигурация proxy
- `proxy-get --admin-email <email>`
- `proxy-set --admin-email <email> [параметры]`

Ключевые параметры:
- `proxy_enabled`
- `proxy_mode` (`global` / `per_provider`)
- `copernicus_via_proxy`, `nasa_via_proxy`
- `bypass_hosts`, `bypass_policy` (`direct` / `force_proxy`)
- `proxy_endpoint` (без секретов в БД)
- `timeout_seconds`, `max_retries`, `backoff_schedule`

### Выполнение запроса провайдера
- `request --provider <Copernicus|NASA|Mock> --module <module> --url <url> [--request-id <id>]`

Область применения proxy:
- только модули `providers/copernicus/*`, `providers/nasa/*`, `datasets/download/*`.

### Health-check / наблюдаемость / деградация
- `health-check --admin-email <email> --provider <source> --module <module> --source-url <url>`
- `request-log --request-id <id> --admin-email <email>`
- `metrics --admin-email <email>`
- `degradation-status --provider <source>`

Классы ошибок:
- `timeout`, `dns`, `tls`, `auth`, `http`, `network`, `unknown`.
