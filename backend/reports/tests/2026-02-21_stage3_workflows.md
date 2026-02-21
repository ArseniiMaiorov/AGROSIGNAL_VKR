# Протокол тестирования этапа 3: контракт, провайдеры, drill-down, экспорт, TTL

Фича: Синхронизация источников
Вход:
  - run sync для Copernicus/NASA/Mock, 1500 часов
Ожидаемый результат:
  - Обновлены last_sync_at/last_success_at и данные доступны
Фактический результат:
  - Copernicus: status=ok, last_success_at=2026-02-21T03:17:20Z; NASA: status=ok, last_success_at=2026-02-21T03:17:26Z; Mock: status=ok, last_success_at=2026-02-21T03:17:31Z
Статус: PASS

Фича: Drill-down по временной шкале
Вход:
  - 2 месяца -> месяц -> день -> часы -> час
Ожидаемый результат:
  - Сводка + bins desc на каждом уровне и точечная статистика на часе
Фактический результат:
  - month_bins=2, day_bins=21, hour_bins=4, point_records=5
Статус: PASS

Фича: Экспорт данных по диапазону
Вход:
  - export-create + export-process для диапазона 60 дней
Ожидаемый результат:
  - Создана задача, сформирован файл, статус ready
Фактический результат:
  - dataset_id=c687716ad0dc4706b241392a9f4fed29, status=ready, file=exports/c687716ad0dc4706b241392a9f4fed29.json
Статус: PASS

Фича: TTL и предупреждение
Вход:
  - dataset expiry=23h -> ttl-check -> dataset-extend + dataset-view
Ожидаемый результат:
  - Предупреждение сформировано, срок продлён, просмотр доступен
Фактический результат:
  - warned=True, extended_count=1, view_bins=31
Статус: PASS
