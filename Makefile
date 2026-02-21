SHELL := /bin/bash

.PHONY: up down lint test ps logs

up:
	$(MAKE) -C backend up

down:
	$(MAKE) -C backend down

lint:
	$(MAKE) -C backend lint

test:
	$(MAKE) -C backend test

ps:
	$(MAKE) -C backend ps

logs:
	$(MAKE) -C backend logs
