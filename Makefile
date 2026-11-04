# Shortcuts for the scripts in scripts/. Every target is also usable without make:
# run the bash command it shows, for example on Windows with Git Bash.
.DEFAULT_GOAL := help
SHELL := bash

EVENTS ?= 5000
RATE ?= 100
SERVICE ?=
# The virtual environment of the README when it exists.
PYTHON ?= $(firstword $(wildcard .venv/bin/python .venv/Scripts/python.exe) python3)

.PHONY: help env build start start-all topics generate produce produce-stop status logs \
	stop clean test test-unit test-integration test-spark test-dags typecheck verify lint

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-17s %s\n", $$1, $$2}'

env: ## Create .env with random secrets (existing values are kept)
	bash scripts/init_env.sh

build: ## Build the Docker images
	docker compose --profile producer build

start: ## Start the stack and wait until it is healthy (producer off)
	bash scripts/start_pipeline.sh

start-all: ## Start the stack with the continuous producer
	bash scripts/start_pipeline.sh --with-producer

topics: ## Create the Kafka topics and list them
	bash scripts/create_topics.sh

generate: ## Send EVENTS events at RATE per second, then stop (make generate EVENTS=10000)
	bash scripts/generate_data.sh --events $(EVENTS) --rate $(RATE)

produce: ## Start the producer in the background at RATE events per second
	bash scripts/generate_data.sh --continuous --rate $(RATE)

produce-stop: ## Stop the background producer
	bash scripts/generate_data.sh --stop

status: ## Show the services and the alerts firing
	bash scripts/status.sh

logs: ## Follow the logs (make logs SERVICE=streaming)
	docker compose --profile producer logs -f --tail 100 $(SERVICE)

stop: ## Stop the stack and keep the data
	bash scripts/stop_pipeline.sh

clean: ## Stop the stack and delete every volume (asks for confirmation)
	bash scripts/stop_pipeline.sh --volumes

test: ## Unit, Spark, DAG tests and type checks
	bash scripts/run_tests.sh all

test-unit: ## Unit tests on the host
	bash scripts/run_tests.sh unit

test-integration: ## Producer tests against a running Kafka
	bash scripts/run_tests.sh integration

test-spark: ## Spark and Delta tests in the Spark image
	bash scripts/run_tests.sh spark

test-dags: ## Airflow DAG tests in the Airflow image
	bash scripts/run_tests.sh dags

typecheck: ## mypy on the host and in the Spark image
	bash scripts/run_tests.sh typecheck

verify: ## End-to-end verification on a running, empty stack
	bash scripts/verify_pipeline.sh

lint: ## Ruff checks and formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
