SHELL := /bin/bash
COMPOSE := docker compose -f docker-compose.yml

.PHONY: build up down ps logs lint test migrate test-stage2 test-stage3 test-stage4 print-port stage3-sync stage3-cycle

build:
	$(COMPOSE) build api

up:
	@API_PORT_RESOLVED=$$(python3 scripts/find_free_port.py --start "$${API_PORT:-18000}"); \
	echo "Выбран порт API: $$API_PORT_RESOLVED"; \
	echo "$$API_PORT_RESOLVED" > .api_port; \
	API_PORT=$$API_PORT_RESOLVED $(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down --remove-orphans
	@rm -f .api_port

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

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

stage3-sync: migrate
	python3 scripts/stage3_cli.py sync --source Copernicus --hours 72
	python3 scripts/stage3_cli.py sync --source NASA --hours 72
	python3 scripts/stage3_cli.py sync --source Mock --hours 72

stage3-cycle: migrate
	python3 scripts/stage3_cli.py run-cycle --hours 24 --retention-days 30

print-port:
	@if [ -f .api_port ]; then \
		echo "API_PORT=$$(cat .api_port)"; \
	else \
		echo "API_PORT не определён. Выполните make up."; \
	fi
