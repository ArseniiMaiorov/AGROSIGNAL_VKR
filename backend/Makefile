SHELL := /bin/bash
COMPOSE := docker compose -f docker-compose.yml
LOCAL_API_PID_FILE := .api_local.pid
LOCAL_API_LOG_FILE := .api_local.log
API_MODE_FILE := .api_mode
SCHEDULER_PID_FILE := .scheduler.pid
SCHEDULER_LOG_FILE := .scheduler.log

.PHONY: build up down ps logs lint test migrate test-stage2 test-stage3 test-stage4 test-stage5 quality print-port stage3-sync stage3-cycle stage4-admin stage4-proxy-get stage4-health stage4-metrics stage4-request-log install-cli

build:
	$(COMPOSE) build api

up:
	@if [ -f $(LOCAL_API_PID_FILE) ] && kill -0 $$(cat $(LOCAL_API_PID_FILE)) 2>/dev/null; then \
		kill $$(cat $(LOCAL_API_PID_FILE)); \
	fi
	@API_PORT_RESOLVED=$$(python3 scripts/find_free_port.py --start "$${API_PORT:-18000}"); \
	echo "Выбран порт API: $$API_PORT_RESOLVED"; \
	echo "$$API_PORT_RESOLVED" > .api_port; \
	rm -f $(API_MODE_FILE); \
	API_PORT=$$API_PORT_RESOLVED $(COMPOSE) up -d --build --wait; \
	rm -f $(LOCAL_API_PID_FILE); \
	if python3 scripts/check_http_health.py --url "http://127.0.0.1:$$API_PORT_RESOLVED/health" --timeout 3 >/dev/null 2>&1; then \
		echo "Доступ к API с хоста: OK"; \
		echo "container" > $(API_MODE_FILE); \
	else \
		echo "Хостовый доступ к published port недоступен, переключаю API в локальный режим."; \
		$(COMPOSE) stop api >/dev/null; \
		for ATTEMPT in $$(seq 1 20); do \
			if python3 scripts/find_free_port.py --start "$$API_PORT_RESOLVED" --max-attempts 1 >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 0.2; \
		done; \
		python3 scripts/spawn_detached.py \
			--pid-file $(LOCAL_API_PID_FILE) \
			--log-file $(LOCAL_API_LOG_FILE) \
			--cwd . \
			--env API_HOST=127.0.0.1 \
			--env API_PORT=$$API_PORT_RESOLVED \
			--env PYTHONPATH=src \
			-- python3 scripts/start_api.py >/dev/null; \
		for ATTEMPT in $$(seq 1 20); do \
			if python3 scripts/check_http_health.py --url "http://127.0.0.1:$$API_PORT_RESOLVED/health" --timeout 1 >/dev/null 2>&1; then \
				echo "local" > $(API_MODE_FILE); \
				echo "Локальный API запущен, PID=$$(cat $(LOCAL_API_PID_FILE))"; \
				break; \
			fi; \
			if ! kill -0 $$(cat $(LOCAL_API_PID_FILE)) 2>/dev/null; then \
				echo "Локальный API завершился с ошибкой. Лог: $(LOCAL_API_LOG_FILE)"; \
				exit 1; \
			fi; \
			sleep 0.2; \
		done; \
		if [ ! -f $(API_MODE_FILE) ] || [ "$$(cat $(API_MODE_FILE))" != "local" ]; then \
			echo "Не удалось подтвердить доступность локального API. Лог: $(LOCAL_API_LOG_FILE)"; \
			exit 1; \
		fi; \
	fi; \
	if [ "$${ENABLE_STAGE_SCHEDULER:-1}" = "1" ]; then \
		if [ ! -f $(SCHEDULER_PID_FILE) ] || ! kill -0 $$(cat $(SCHEDULER_PID_FILE)) 2>/dev/null; then \
			python3 scripts/spawn_detached.py \
				--pid-file $(SCHEDULER_PID_FILE) \
				--log-file $(SCHEDULER_LOG_FILE) \
				--cwd . \
				--env STAGE_SCHEDULER_INTERVAL_SECONDS=$${STAGE_SCHEDULER_INTERVAL_SECONDS:-900} \
				--env STAGE_SCHEDULER_SYNC_HOURS=$${STAGE_SCHEDULER_SYNC_HOURS:-24} \
				--env STAGE_SCHEDULER_RETENTION_DAYS=$${STAGE_SCHEDULER_RETENTION_DAYS:-30} \
				-- python3 scripts/stage_scheduler.py >/dev/null; \
			sleep 1; \
			if [ -f $(SCHEDULER_PID_FILE) ] && kill -0 $$(cat $(SCHEDULER_PID_FILE)) 2>/dev/null; then \
				echo "Фоновый планировщик запущен, PID=$$(cat $(SCHEDULER_PID_FILE))"; \
			else \
				echo "Не удалось запустить планировщик. Лог: $(SCHEDULER_LOG_FILE)"; \
				exit 1; \
			fi; \
		else \
			echo "Фоновый планировщик уже запущен, PID=$$(cat $(SCHEDULER_PID_FILE))"; \
		fi; \
	else \
		echo "Фоновый планировщик отключён (ENABLE_STAGE_SCHEDULER=0)"; \
	fi

down:
	@if [ -f $(LOCAL_API_PID_FILE) ] && kill -0 $$(cat $(LOCAL_API_PID_FILE)) 2>/dev/null; then \
		kill $$(cat $(LOCAL_API_PID_FILE)); \
	fi
	@if [ -f $(SCHEDULER_PID_FILE) ] && kill -0 $$(cat $(SCHEDULER_PID_FILE)) 2>/dev/null; then \
		kill $$(cat $(SCHEDULER_PID_FILE)); \
	fi
	$(COMPOSE) down --remove-orphans
	@rm -f .api_port $(LOCAL_API_PID_FILE) $(LOCAL_API_LOG_FILE) $(API_MODE_FILE) $(SCHEDULER_PID_FILE) $(SCHEDULER_LOG_FILE)

ps:
	$(COMPOSE) ps
	@if [ -f $(API_MODE_FILE) ]; then \
		echo "API_MODE=$$(cat $(API_MODE_FILE))"; \
	fi
	@if [ -f $(LOCAL_API_PID_FILE) ] && kill -0 $$(cat $(LOCAL_API_PID_FILE)) 2>/dev/null; then \
		echo "LOCAL_API_PID=$$(cat $(LOCAL_API_PID_FILE)) (running)"; \
	fi
	@if [ -f $(SCHEDULER_PID_FILE) ] && kill -0 $$(cat $(SCHEDULER_PID_FILE)) 2>/dev/null; then \
		echo "SCHEDULER_PID=$$(cat $(SCHEDULER_PID_FILE)) (running)"; \
	fi

logs:
	@if [ -f $(API_MODE_FILE) ] && [ "$$(cat $(API_MODE_FILE))" = "local" ]; then \
		touch $(LOCAL_API_LOG_FILE) $(SCHEDULER_LOG_FILE); \
		tail -f -n 200 $(LOCAL_API_LOG_FILE) $(SCHEDULER_LOG_FILE); \
	else \
		$(COMPOSE) logs -f --tail=200; \
	fi

lint: build
	$(COMPOSE) run --rm --no-deps api python -m compileall -q src tests scripts

test: build
	$(COMPOSE) run --rm --no-deps api python scripts/run_tests_with_coverage.py

migrate:
	python3 scripts/migrate.py

test-stage2: migrate
	python3 scripts/test_stage2_geometry.py

test-stage3: migrate
	python3 scripts/test_stage3_workflows.py

test-stage4: migrate
	python3 scripts/test_stage4_proxy.py

test-stage5: migrate
	python3 scripts/test_stage5_api.py

quality:
	python3 scripts/run_quality_gate.py

stage3-sync: migrate
	python3 scripts/stage3_cli.py sync --source Copernicus --hours 72
	python3 scripts/stage3_cli.py sync --source NASA --hours 72
	python3 scripts/stage3_cli.py sync --source Mock --hours 72

stage3-cycle: migrate
	python3 scripts/stage3_cli.py run-cycle --hours 24 --retention-days 30

stage4-admin:
	python3 scripts/stage4_cli.py ensure-admin --email "$${ADMIN_EMAIL:-admin.stage4@zemledar.local}"

stage4-proxy-get:
	python3 scripts/stage4_cli.py proxy-get --admin-email "$${ADMIN_EMAIL:-admin.stage4@zemledar.local}"

stage4-health:
	@API_PORT_RESOLVED=$$(cat .api_port 2>/dev/null); \
	if [ -z "$$API_PORT_RESOLVED" ]; then \
		echo "API_PORT не определён. Выполните make up."; \
		exit 1; \
	fi; \
	python3 scripts/stage4_cli.py health-check \
		--admin-email "$${ADMIN_EMAIL:-admin.stage4@zemledar.local}" \
		--provider "$${PROVIDER:-Copernicus}" \
		--module "$${MODULE_NAME:-providers/copernicus/sync}" \
		--source-url "http://127.0.0.1:$$API_PORT_RESOLVED/health"

stage4-metrics:
	python3 scripts/stage4_cli.py metrics --admin-email "$${ADMIN_EMAIL:-admin.stage4@zemledar.local}"

stage4-request-log:
	@if [ -z "$$REQUEST_ID" ]; then \
		echo "Укажите REQUEST_ID=<id>"; \
		exit 1; \
	fi; \
	python3 scripts/stage4_cli.py request-log --request-id "$$REQUEST_ID" --admin-email "$${ADMIN_EMAIL:-admin.stage4@zemledar.local}"

install-cli:
	@mkdir -p "$$HOME/.local/bin"
	@ln -sf "$$(cd .. && pwd)/stage4_cli.py" "$$HOME/.local/bin/stage4_cli.py"
	@chmod +x "$$HOME/.local/bin/stage4_cli.py"
	@echo "Установлено: $$HOME/.local/bin/stage4_cli.py"

print-port:
	@if [ -f .api_port ]; then \
		echo "API_PORT=$$(cat .api_port)"; \
		if [ -f $(API_MODE_FILE) ]; then \
			echo "API_MODE=$$(cat $(API_MODE_FILE))"; \
		fi; \
	else \
		echo "API_PORT не определён. Выполните make up."; \
	fi
