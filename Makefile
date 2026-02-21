SHELL := /bin/bash

.PHONY: up down lint test migrate test-stage2 test-stage3 test-stage4 test-stage5 quality stage3-sync stage3-cycle ps logs print-port stage4-admin stage4-proxy-get stage4-health stage4-metrics stage4-request-log install-cli

up:
	$(MAKE) -C backend up

down:
	$(MAKE) -C backend down

lint:
	$(MAKE) -C backend lint

test:
	$(MAKE) -C backend test

migrate:
	$(MAKE) -C backend migrate

test-stage2:
	$(MAKE) -C backend test-stage2

test-stage3:
	$(MAKE) -C backend test-stage3

test-stage4:
	$(MAKE) -C backend test-stage4

test-stage5:
	$(MAKE) -C backend test-stage5

quality:
	$(MAKE) -C backend quality

stage3-sync:
	$(MAKE) -C backend stage3-sync

stage3-cycle:
	$(MAKE) -C backend stage3-cycle

ps:
	$(MAKE) -C backend ps

logs:
	$(MAKE) -C backend logs

print-port:
	$(MAKE) -C backend print-port

stage4-admin:
	$(MAKE) -C backend stage4-admin

stage4-proxy-get:
	$(MAKE) -C backend stage4-proxy-get

stage4-health:
	$(MAKE) -C backend stage4-health

stage4-metrics:
	$(MAKE) -C backend stage4-metrics

stage4-request-log:
	$(MAKE) -C backend stage4-request-log

install-cli:
	$(MAKE) -C backend install-cli
