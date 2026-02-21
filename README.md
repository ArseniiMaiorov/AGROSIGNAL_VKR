# ИСМ «Земледар»

Монорепозиторий дипломного проекта (этап 1): базовый каркас, Python backend, Docker-окружение и автоматические проверки качества.

## Что уже готово на этапе 1
- Каркас репозитория `backend/frontend/docs`.
- Backend на Python с endpoint `GET /health`.
- Docker Compose со стеком: `api + postgis + redis`.
- Команды качества: `make up`, `make lint`, `make test`.
- Покрытие backend-тестами: 100% для текущего кода в `backend/src`.

## Требования
- Docker + Docker Compose v2
- GNU Make

## Быстрый старт
```bash
make up
make lint
make test
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
curl http://127.0.0.1:18000/health
```

Надёжная проверка из контейнера API:
```bash
cd backend
docker compose -f docker-compose.yml exec -T api python - <<'PY'
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).read().decode())
PY
```
