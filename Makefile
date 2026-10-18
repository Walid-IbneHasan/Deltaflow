UV      ?= uv
COMPOSE ?= docker compose

# `make load` runs a finite workload by default so it terminates;
# use LOAD_ARGS="--txns 0" to run until interrupted.
LOAD_ARGS ?= --txns 2000
SEED_ARGS ?=

.PHONY: up down nuke psql seed load tail tail-drop views differential lint fmt typecheck test check

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

# Print the decoded change stream (persistent slot; resumes after Ctrl-C).
tail:
	$(UV) run python -m deltaflow.replication.tail

# Drop tail's slot so it stops retaining WAL.
tail-drop:
	$(UV) run python -m deltaflow.replication.tail --drop

# Maintain a filtered, a join and an aggregate view and print their changes (run `make load` alongside).
views:
	$(UV) run python -m deltaflow.devtools.views_demo

# Differential acceptance tests (M1 replay, M2 views). Random seed unless DELTAFLOW_SEED is set.
differential:
	$(UV) run pytest tests/differential -v -s

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
