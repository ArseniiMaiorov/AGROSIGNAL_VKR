# Протокол тестирования этапа 6: алгоритмы и расчёты (агро-логика)

Фича: Подготовка поля для stage6
Вход:
  - POST /api/v1/fields
Ожидаемый результат:
  - Поле создано
Фактический результат:
  - status=201, field_id=6
Статус: PASS

Фича: Golden fixture: GDD
Вход:
  - GET /fields/{id}/algorithms/gdd
Ожидаемый результат:
  - status=OK и значения в пределах допусков
Фактический результат:
  - status=200, derived_status=OK, values=[{'date': '2026-02-19', 'tmean_c': 17.0, 'gdd_day': 7.0, 'gdd_accum': 7.0}, {'date': '2026-02-20', 'tmean_c': 15.75, 'gdd_day': 5.75, 'gdd_accum': 12.75}]
Статус: PASS

Фича: Golden fixture: VPD
Вход:
  - GET /fields/{id}/algorithms/vpd?granularity=day
Ожидаемый результат:
  - status=OK и значения VPD в пределах допусков
Фактический результат:
  - status=200, derived_status=OK, values=[{'date': '2026-02-19', 'vpd_kpa': 0.88276, 'vpd_min_kpa': 0.420769, 'vpd_max_kpa': 1.454162}, {'date': '2026-02-20', 'vpd_kpa': 0.717321, 'vpd_min_kpa': 0.328179, 'vpd_max_kpa': 1.169141}]
Статус: PASS

Фича: Golden fixture: ET0
Вход:
  - GET /fields/{id}/algorithms/et0?granularity=day
Ожидаемый результат:
  - status=OK, корректный variant и значения в пределах допусков
Фактический результат:
  - status=200, variant=fao56_simplified, values=[{'date': '2026-02-19', 'et0_mm_day': 5.333746, 'et0_min_mm_day': 0.86248, 'et0_max_mm_day': 11.977196}, {'date': '2026-02-20', 'et0_mm_day': 5.226898, 'et0_min_mm_day': 0.819664, 'et0_max_mm_day': 11.613862}]
Статус: PASS

Фича: Golden fixture: Water deficit
Вход:
  - GET /fields/{id}/algorithms/water-deficit
Ожидаемый результат:
  - status=OK и корректная дельта ET0_sum - precip_sum
Фактический результат:
  - status=200, derived={'status': 'OK', 'reason': None, 'algorithm_id': 'water_deficit', 'algorithm_version': 'algorithms.v1', 'inputs_used': {'metrics': ['precipitation', 'et0'], 'formula': 'et0_sum - precip_sum'}, 'quality_summary': {'total_points': 32, 'quality_flags': {}, 'low_quality_points': 0, 'confidence': 'high'}, 'values': [{'from': '2026-02-19T00:00:00Z', 'to': '2026-02-20T18:00:00Z', 'et0_sum_mm': 10.560644, 'precip_sum_mm': 4.0, 'water_deficit_mm': 6.560644}]}
Статус: PASS

Фича: Пространственные тесты: zoom-детализация
Вход:
  - GET /layers/{layer_id}/grid на zoom 9/11/13/15
Ожидаемый результат:
  - cell_size_m = 1000/500/250/100 + meta режима
Фактический результат:
  - z9:1000, z11:500, z13:250, z15:100
Статус: PASS

Фича: Пространственные тесты: probe
Вход:
  - GET /fields/{id}/probe
Ожидаемый результат:
  - Возвращается значение для запрошенных слоёв + mini_stats/mini_reco
Фактический результат:
  - status=200, values=2, mini_stats={'precipitation_sum_24h_mm': 2.3, 'wind_avg_6h_ms': 3.75}
Статус: PASS

Фича: Пространственные тесты: zones/zonal-stats
Вход:
  - GET /zones + GET /zonal-stats
Ожидаемый результат:
  - Зоны формируются, статистика по зонам доступна
Фактический результат:
  - zones=25, zonal_items=1
Статус: PASS

Фича: Негативные тесты: INSUFFICIENT_DATA
Вход:
  - GET /algorithms/gdd на пустом диапазоне
Ожидаемый результат:
  - status=INSUFFICIENT_DATA + причина
Фактический результат:
  - status=200, derived={'status': 'INSUFFICIENT_DATA', 'reason': 'Недостаточно данных: отсутствуют температуры', 'algorithm_id': 'gdd', 'algorithm_version': 'algorithms.v1', 'inputs_used': {'metrics': ['temperature'], 'tbase_c': 10.0}, 'quality_summary': {'total_points': 0, 'quality_flags': {}, 'low_quality_points': 0, 'confidence': 'high'}, 'values': []}
Статус: PASS

Фича: Негативные тесты: VALIDATION_ERROR (единицы)
Вход:
  - GET /algorithms/gdd при unit=K
Ожидаемый результат:
  - 422 VALIDATION_ERROR
Фактический результат:
  - status=422, error={'code': 'VALIDATION_ERROR', 'message': 'Некорректные входные данные: единицы temperature (K), ожидается C', 'details': None}
Статус: PASS

Фича: Негативные тесты: LOW_QUALITY
Вход:
  - GET /satellite/quality с флагом low_quality
Ожидаемый результат:
  - Данные остаются видимыми, но помечаются LOW_QUALITY
Фактический результат:
  - status=200, quality_items=24, has_low_quality=True
Статус: PASS

Фича: Моделирование: baseline vs scenario
Вход:
  - POST scenario + POST run + GET diff
Ожидаемый результат:
  - Есть дельта по водным метрикам и пересчёт рекомендаций/derived
Фактический результат:
  - create=201, run=202, diff=200, has_precip_delta=True, has_water_diff=True
Статус: PASS

Фича: Моделирование: сценарий вне диапазона
Вход:
  - POST /modeling/scenarios с from за пределами retention
Ожидаемый результат:
  - 422 VALIDATION_ERROR
Фактический результат:
  - status=422, error={'code': 'VALIDATION_ERROR', 'message': 'Некорректные входные данные: диапазон вне доступного хранения', 'details': None}
Статус: PASS

Фича: Моделирование: сценарий без baseline
Вход:
  - POST /modeling/scenarios baseline_id=''
Ожидаемый результат:
  - 422 VALIDATION_ERROR
Фактический результат:
  - status=422, error={'code': 'VALIDATION_ERROR', 'message': 'Некорректные входные данные: сценарий без baseline недопустим', 'details': None}
Статус: PASS

## Сводка
Всего тестов: 14
PASS: 14
FAIL: 0
