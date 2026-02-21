# Протокол тестирования этапа 3: контракт, провайдеры, drill-down, экспорт, TTL

Фича: Синхронизация источников
Вход:
  - run sync для Copernicus/NASA/Mock, 1500 часов
Ожидаемый результат:
  - Обновлены last_sync_at/last_success_at и данные доступны
Фактический результат:
  - Copernicus: status=ok, last_success_at=2026-02-21T07:37:26Z; NASA: status=ok, last_success_at=2026-02-21T07:37:32Z; Mock: status=ok, last_success_at=2026-02-21T07:37:39Z
Статус: PASS

Фича: Drill-down по временной шкале
Вход:
  - 2 месяца -> месяц -> день -> часы -> час
Ожидаемый результат:
  - Сводка + bins desc на каждом уровне и точечная статистика на часе
Фактический результат:
  - month_bins=2, day_bins=21, hour_bins=8, point_records=13
Статус: PASS

Фича: Экспорт данных по диапазону
Вход:
  - export-create + export-process для диапазона 60 дней
Ожидаемый результат:
  - Создана задача, сформирован файл, статус ready
Фактический результат:
  - dataset_id=e59f789be38d414e9f08ecb890fe1c93, status=ready, file=exports/e59f789be38d414e9f08ecb890fe1c93.json
Статус: PASS

Фича: TTL и предупреждение
Вход:
  - dataset expiry=23h -> ttl-check -> dataset-extend + dataset-view
Ожидаемый результат:
  - Предупреждение сформировано, срок продлён, просмотр доступен
Фактический результат:
  - warned=True, extended_count=1, view_bins=31
Статус: PASS
