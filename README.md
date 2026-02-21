# ИСМ «Земледар»

Монорепозиторий дипломного проекта.

Текущий статус: завершены этапы 1-2
- Этап 1: каркас, Docker-окружение, базовый backend, автотесты.
- Этап 2: модель данных PostGIS, миграции, валидация геометрии полей, интеграционные тесты фикстур.

## Требования
- Docker + Docker Compose v2
- GNU Make
- Python 3 (для локальных служебных скриптов миграций/интеграционных тестов)

## Быстрый запуск
```bash
make up
make print-port
```

Команда `make up` автоматически выбирает свободный порт для API, если целевой порт занят.

## Проверки качества
```bash
make lint
make test
```

## Этап 2: миграции и интеграционные тесты
```bash
make migrate
make test-stage2
```

## Полезные команды
```bash
make ps
make logs
make down
```

## Проверка API
С хоста:
```bash
curl http://127.0.0.1:<PORT>/health
```
где `<PORT>` можно получить через:
```bash
make print-port
```

Проверка из контейнера API:
```bash
cd backend
docker compose -f docker-compose.yml exec -T api python - <<'PY'
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).read().decode())
PY
```
