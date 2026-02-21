# ИСМ «Земледар»

Монорепозиторий дипломного проекта.

Текущий статус: завершены этапы 1-3.
- Этап 1: каркас, Docker-окружение, базовый backend, автотесты.
- Этап 2: модель данных PostGIS, миграции, валидация геометрии полей.
- Этап 3: единый контракт данных, провайдеры Copernicus/NASA/Mock, sync/drill-down/export/TTL.

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
```

## Режимы этапа 3
```bash
make stage3-sync
make stage3-cycle
```

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
