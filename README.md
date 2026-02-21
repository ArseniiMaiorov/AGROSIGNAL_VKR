# ИСМ «Земледар»

Монорепозиторий дипломного проекта.

Текущий статус: завершены этапы 1-5.
- Этап 1: каркас, Docker-окружение, базовый backend, автотесты.
- Этап 2: модель данных PostGIS, миграции, валидация геометрии полей.
- Этап 3: единый контракт данных, провайдеры Copernicus/NASA/Mock, sync/drill-down/export/TTL.
- Этап 4: proxy-контур загрузчиков датасетов, health-check, ретраи, наблюдаемость, режим деградации.
- Этап 5: полнофункциональный API `/api/v1` (CRUD домена, RBAC, аудит, weather/satellite, assistant, export, idempotency, метрики).

## Требования
- Docker + Docker Compose v2
- GNU Make
- Python 3

## Быстрый запуск
```bash
make up
make print-port
```

`make up` автоматически выбирает свободный порт API, если целевой порт занят.
Если в локальной среде недоступен Docker published port, `make up` автоматически переключает API в локальный режим (`API_MODE=local`) и сохраняет порт в `backend/.api_port`.

## Базовые проверки качества
```bash
make lint
make test
make migrate
make test-stage2
make test-stage3
make test-stage4
make test-stage5
make quality
```

## Режимы этапа 3
```bash
make stage3-sync
make stage3-cycle
```

## Режимы этапа 4 (proxy)
```bash
cd backend
python3 scripts/stage4_cli.py ensure-admin --email admin.stage4@zemledar.local
python3 scripts/stage4_cli.py proxy-get --admin-email admin.stage4@zemledar.local
python3 scripts/stage4_cli.py health-check \
  --admin-email admin.stage4@zemledar.local \
  --provider Copernicus \
  --module providers/copernicus/sync \
  --source-url https://example.org/
python3 scripts/stage4_cli.py metrics --admin-email admin.stage4@zemledar.local
```

Упрощённый вызов из корня репозитория:
```bash
./stage4_cli.py proxy-get --admin-email admin.stage4@zemledar.local
```

Если нужен вызов без `./`, установите wrapper в `~/.local/bin`:
```bash
make install-cli
stage4_cli.py proxy-get --admin-email admin.stage4@zemledar.local
```

Секреты proxy (логин/пароль/token) хранятся в переменных окружения или secret-store и не сохраняются в БД.
Прокси применяется только к outbound-запросам модулей `providers/copernicus/*`, `providers/nasa/*`, `datasets/download/*`.
Для операций чтения/изменения proxy-настроек, журналов и метрик требуется роль `admin`.

## Полезные команды
```bash
make ps
make logs
make down
```

## Проверка API
```bash
API_PORT=$(cat backend/.api_port)
curl -sS "http://127.0.0.1:${API_PORT}/health"
```

## Этап 5: API v1 (без фронта)
Базовая авторизация для сценариев этапа 5 выполняется по заголовку `X-User-Email`.

Тестовые пользователи, создаваемые seed-механизмом:
- `admin@zemledar.local`
- `manager@zemledar.local`
- `agronomist@zemledar.local`
- `viewer@zemledar.local`

Быстрые проверки:
```bash
API_PORT=$(cat backend/.api_port)

curl -sS -H "X-User-Email: manager@zemledar.local" \
  "http://127.0.0.1:${API_PORT}/api/v1/auth/me"

curl -sS -H "X-User-Email: manager@zemledar.local" \
  "http://127.0.0.1:${API_PORT}/api/v1/metrics/overview"

make test-stage5
```
