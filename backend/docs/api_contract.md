# API/режимы backend (этапы 1-3)

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

### Экспорт
- `export-create --source <source> --field-id <id> --from <ISO> --to <ISO> --granularity <g> --format <json|csv>`
- `export-process [--dataset-id <id>]`
- `export-status --dataset-id <id>`

### TTL
- `ttl-check`
- `dataset-extend --dataset-id <id> --days <N>`
- `dataset-view --dataset-id <id> [--granularity <g>]`
