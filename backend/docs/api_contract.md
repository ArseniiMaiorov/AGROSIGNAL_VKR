# API/режимы backend (этапы 1-4)

## HTTP endpoint

### GET /health

Назначение:
- Проверка доступности API и базовой конфигурации окружения.

Успешный ответ `200 OK`:
```json
{
  "status": "ok",
  "service": "zemledar-api",
  "environment": "dev",
  "timestamp": "2026-02-21T12:00:00+00:00"
}
```

## Режимы backend (этап 3)

Режимы реализованы через `scripts/stage3_cli.py` и используются как контракт backend-слоя.

### Синхронизация
- `sync --source <Copernicus|NASA|Mock> --hours <N> [--field-id <id>]`
- `sync-status --source <Copernicus|NASA|Mock>`

### Drill-down и диапазоны
- `query --source <source> --field-id <id> --from <ISO> --to <ISO> --granularity <month|day|hour|point>`

Ответ включает:
- `summary` по диапазону,
- `time_bins` следующего уровня (desc),
- `records` в едином контракте (`value/unit/timestamp/source/quality_flags/meta`).

Поддерживаемые метрики:
- `precipitation`, `temperature`, `wind_speed`, `cloudiness`,
- `ndvi`, `ndre`, `ndmi`, `cloud_mask`.

### Экспорт
- `export-create --source <source> --field-id <id> --from <ISO> --to <ISO> --granularity <g> --format <json|csv>`
- `export-process [--dataset-id <id>]`
- `export-status --dataset-id <id>`

### TTL
- `ttl-check`
- `dataset-extend --dataset-id <id> --days <N>`
- `dataset-view --dataset-id <id> [--granularity <g>]`

## Режимы backend (этап 4: proxy)

Режимы реализованы через `scripts/stage4_cli.py`.

### Admin/RBAC
- `ensure-admin --email <admin@email>`
- `proxy-set` доступен только для пользователя с ролью `admin`.

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

Ответ включает:
- `proxy_used`, `proxy_reason`
- `http_status`, `duration_ms`, `bytes_downloaded`
- `error_class`, `retry_count`

### Health-check proxy/source
- `health-check --admin-email <email> --provider <source> --module <module> --source-url <url>`

Обязательные поля ответа:
- `proxy_enabled`
- `proxy_last_check_at`
- `proxy_check_result` (`status`, `reason`, `latency_ms`, `error_class`)
- `source_reachability` (`status`, `http_status`, `duration_ms`, `error_class`, `reason`)

### Наблюдаемость и деградация
- `request-log --request-id <id> --admin-email <email>`
- `metrics --admin-email <email>`
- `degradation-status --provider <source>`

Классы ошибок:
- `timeout`, `dns`, `tls`, `auth`, `http`, `network`, `unknown`
