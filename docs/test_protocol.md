# Протокол тестирования

## Этап 1
Проверки:
1. `make up` — контейнеры запускаются и переходят в `healthy`.
2. `make lint` — проверка синтаксиса backend-кода проходит без ошибок.
3. `make test` — unit-тесты проходят, покрытие 100% по `backend/src`.

## Этап 2
Проверки:
1. `make migrate` — миграции PostGIS применяются без ошибок.
2. `make test-stage2` — интеграционные проверки геофикстур:
   - `field_ok.geojson` -> PASS;
   - `field_self_intersect.geojson` -> ожидаемая ошибка «Полигон самопересекается»;
   - `field_wrong_srid.geojson` -> ожидаемая ошибка «Неверная система координат».
3. Формируется отчёт `backend/reports/tests/<дата>_stage2_geometry.md`.
