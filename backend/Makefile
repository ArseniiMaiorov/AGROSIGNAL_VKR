SHELL := /bin/bash
COMPOSE := docker compose -f docker-compose.yml

.PHONY: build up down ps logs lint test

build:
	$(COMPOSE) build api

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down --remove-orphans

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

lint: build
	$(COMPOSE) run --rm --no-deps api python -m compileall -q src tests scripts

test: build
	$(COMPOSE) run --rm --no-deps api python scripts/run_tests_with_coverage.py
