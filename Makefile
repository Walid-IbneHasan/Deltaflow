UV      ?= uv
COMPOSE ?= docker compose

# `make load` runs a finite workload by default so it terminates;
# use LOAD_ARGS="--txns 0" to run until interrupted.
LOAD_ARGS ?= --txns 2000
SEED_ARGS ?=

.PHONY: up down nuke psql seed load lint fmt typecheck test check

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

# Destroys the Postgres volume (all data, all replication slots).
nuke:
	$(COMPOSE) down -v

psql:
	$(COMPOSE) exec postgres psql -U deltaflow -d deltaflow

seed:
	$(UV) run python -m deltaflow.devtools.seed $(SEED_ARGS)

load:
	$(UV) run python -m deltaflow.devtools.loadgen $(LOAD_ARGS)

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt:
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest

check: lint typecheck test
