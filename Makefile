# Task runner for Linux / macOS / WSL. On Windows PowerShell use ./make.ps1 <task>.
SHELL := /bin/bash
CITY  ?= JC
START ?= 2025-01
END   ?= 2025-12
RUN   := docker compose run --rm pipelines

.PHONY: help setup env build up-core infra bootstrap up deploy-flows backfill dbt-build dbt-docs bruin lint test status logs down destroy lock

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

env:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

setup: build up-core infra bootstrap up deploy-flows ## Build, provision and start everything
	@echo -e "\nKestra http://localhost:8080 | Dashboard http://localhost:8501 | MinIO http://localhost:9001 | Kafka UI http://localhost:8081"
	@echo "Next: make backfill CITY=JC START=2025-01 END=2025-12"

build: env ## Build the pipelines image
	docker compose build pipelines

up-core: env ## Start Postgres, MinIO and Kafka
	docker compose up -d --wait postgres minio kafka

infra: ## Terraform: lake bucket, warehouse schemas, read-only role
	docker compose run --rm terraform init -input=false
	docker compose run --rm terraform apply -auto-approve -input=false

bootstrap: ## Create warehouse source tables
	$(RUN) python -m bikeshare.warehouse.bootstrap

up: env ## Start Kestra, streaming services and dashboard
	docker compose up -d --wait kestra kafka-ui producer consumer-lake consumer-live dashboard

deploy-flows: ## Deploy Kestra flows from kestra/flows
	$(RUN) python -m bikeshare.tools.kestra deploy

backfill: ## Run the end-to-end platform_backfill flow (CITY, START, END)
	$(RUN) python -m bikeshare.tools.kestra run platform_backfill \
		--input city=$(CITY) --input start_month=$(START) --input end_month=$(END) --wait

dbt-build: ## dbt seed + build
	$(RUN) sh -c "cd /app/dbt && dbt seed && dbt build"

dbt-docs: ## Serve dbt docs on http://localhost:8082
	docker compose run --rm --service-ports pipelines sh -c "cd /app/dbt && dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8082"

bruin: ## Run the Bruin pipeline
	$(RUN) bash /app/bruin/run.sh

lint: ## ruff
	$(RUN) ruff check src dashboard tests

test: lint ## Lint, unit tests, dbt parse, terraform validate
	$(RUN) pytest -q /app/tests
	$(RUN) sh -c "cd /app/dbt && dbt parse --no-partial-parse"
	docker compose run --rm terraform validate

status: ## Service status
	docker compose ps

logs: ## Follow streaming logs
	docker compose logs -f --tail 50 producer consumer-lake consumer-live

down: ## Stop everything (keeps data)
	docker compose down

destroy: ## Stop everything and DELETE all data volumes
	@read -p "Delete ALL data volumes? [yes/N] " ans && [ "$$ans" = "yes" ]
	docker compose --profile tools down -v
	rm -rf infra/terraform/.terraform infra/terraform/terraform.tfstate*

lock: ## Re-lock Python dependencies
	docker run --rm -v "$(PWD)/docker/pipelines:/w" -w /w python:3.12-slim-bookworm sh -c \
		"pip install -q uv && uv pip compile requirements.txt --python-version 3.12 --python-platform x86_64-manylinux_2_28 -o requirements.lock"
