# ИСМ «Земледар»

Монорепозиторий дипломного проекта.

Текущий статус: завершены этапы 1-4.
- Этап 1: каркас, Docker-окружение, базовый backend, автотесты.
- Этап 2: модель данных PostGIS, миграции, валидация геометрии полей.
- Этап 3: единый контракт данных, провайдеры Copernicus/NASA/Mock, sync/drill-down/export/TTL.
- Этап 4: proxy-контур загрузчиков датасетов, health-check, ретраи, наблюдаемость, режим деградации.

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

## Базовые проверки качества
```bash
make lint
make test
make migrate
make test-stage2
make test-stage3
make test-stage4
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
python3 scripts/stage4_cli.py proxy-get
python3 scripts/stage4_cli.py health-check \
  --admin-email admin.stage4@zemledar.local \
  --provider Copernicus \
  --module providers/copernicus/sync \
  --source-url https://example.org/
```

Секреты proxy (логин/пароль/token) хранятся в переменных окружения или secret-store и не сохраняются в БД.
Прокси применяется только к outbound-запросам модулей `providers/copernicus/*`, `providers/nasa/*`, `datasets/download/*`.

## Полезные команды
```bash
make ps
make logs
make down
```

## Проверка API
```bash
cd backend
docker compose -f docker-compose.yml exec -T api python - <<'PY'
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).read().decode())
PY
```
