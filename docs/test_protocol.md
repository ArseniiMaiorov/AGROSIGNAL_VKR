# Протокол тестирования

## Этап 1
1. `make up` — контейнеры запускаются и переходят в `healthy`.
2. `make lint` — проверка синтаксиса backend-кода проходит без ошибок.
3. `make test` — unit-тесты проходят, покрытие 100% по `backend/src`.

## Этап 2
1. `make migrate` — миграции PostGIS применяются без ошибок.
2. `make test-stage2` — проверки геофикстур:
   - `field_ok.geojson` -> PASS;
   - `field_self_intersect.geojson` -> ошибка «Полигон самопересекается»;
   - `field_wrong_srid.geojson` -> ошибка «Неверная система координат».

## Этап 3
1. `make test-stage3` — проверяются сценарии:
   - синхронизация Copernicus/NASA/Mock + last_sync;
   - drill-down (месяцы -> дни -> часы -> час);
   - экспорт диапазона (создание задачи, обработка, готовый файл);
   - TTL (предупреждение за 1 день, продление, повторный просмотр dataset).
2. Формируется отчёт:
   - `backend/reports/tests/<дата>_stage3_workflows.md`.

## Этап 4
1. `make test-stage4` — проверяются сценарии:
   - Proxy ON + 401 (неверные креды) -> FAIL без ретраев;
   - Proxy ON + DNS/TLS ошибки -> FAIL с корректным `error_class`;
   - Proxy ON + 429 -> ретраи по backoff и успех/контролируемый FAIL;
   - `per_provider` режим (Copernicus через proxy, NASA напрямую);
   - `bypass_hosts` (домен из списка идёт напрямую при proxy ON);
   - корректная фиксация `proxy_used=true/false` в журнале;
   - health-check возвращает статус proxy/source и время проверки;
   - деградационный статус источника содержит понятную причину.
2. Формируется отчёт:
   - `backend/reports/tests/<дата>_stage4_proxy.md`.
