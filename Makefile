SHELL := /bin/bash

.PHONY: up down lint test migrate test-stage2 test-stage3 stage3-sync stage3-cycle ps logs print-port

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
