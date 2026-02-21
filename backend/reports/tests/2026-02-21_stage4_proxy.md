# Протокол тестирования этапа 4: proxy-контур, ретраи, наблюдаемость

Фича: Proxy ON + неверные креды
Вход:
  - proxy_enabled=true, source=Copernicus, proxy отвечает 401
Ожидаемый результат:
  - FAIL, error_class=auth, retry_count=0
Фактический результат:
  - success=False, http_status=401, error_class=auth, retry_count=0
Статус: PASS

Фича: Proxy ON + DNS ошибка
Вход:
  - proxy_endpoint=nonexistent-proxy.invalid, запрос Copernicus
Ожидаемый результат:
  - FAIL, error_class=dns
Фактический результат:
  - success=False, error_class=dns
Статус: PASS

Фича: Режим деградации
Вход:
  - degradation-status --provider Copernicus после failed sync
Ожидаемый результат:
  - degradation_mode=true, понятная причина для UI
Фактический результат:
  - status=error, degradation_mode=True, message=Источник недоступен. Показаны данные последней успешной синхронизации. Причина: dns: <urlopen error [Errno -2] Name or service not known>
Статус: PASS

Фича: Proxy ON + TLS ошибка сертификата
Вход:
  - proxy ON + bypass 127.0.0.1 + запрос https к http-источнику
Ожидаемый результат:
  - FAIL, error_class=tls
Фактический результат:
  - success=False, error_class=tls
Статус: PASS

Фича: Proxy ON + источник 429
Вход:
  - proxy отвечает 429,429,200; max_retries=3
Ожидаемый результат:
  - PASS после ретраев (retry_count=2) либо контролируемый FAIL
Фактический результат:
  - success=True, retry_count=2, http_status=200, calls=3
Статус: PASS

Фича: Per-provider режим
Вход:
  - mode=per_provider, Copernicus=true, NASA=false
Ожидаемый результат:
  - Copernicus -> proxy_used=true, NASA -> proxy_used=false, оба PASS
Фактический результат:
  - cop_success=True, cop_proxy=True; nasa_success=True, nasa_proxy=False
Статус: PASS

Фича: Bypass list
Вход:
  - proxy ON + bypass_hosts=localhost + proxy недоступен
Ожидаемый результат:
  - Запрос к localhost выполняется напрямую (proxy_used=false) и PASS
Фактический результат:
  - success=True, proxy_used=False
Статус: PASS

Фича: Логирование proxy_used
Вход:
  - request-log для stage4-perprovider-cop и stage4-perprovider-nasa
Ожидаемый результат:
  - В логе Copernicus proxy_used=true, NASA proxy_used=false
Фактический результат:
  - cop_proxy=True, nasa_proxy=False
Статус: PASS

Фича: Health-check proxy/source
Вход:
  - health-check --provider NASA --module providers/nasa/sync
Ожидаемый результат:
  - proxy_enabled/proxy_last_check_at/proxy_check_result/source_reachability в ответе
Фактический результат:
  - proxy_enabled=False, proxy_result=OK, source_status=OK
Статус: PASS
