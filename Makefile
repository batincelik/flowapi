.PHONY: dev up down build migrate migration test lint format demo clean
dev:
	docker compose up --build
up:
	docker compose up -d
down:
	docker compose down
build:
	docker compose build
migrate:
	.venv/bin/alembic upgrade head
migration:
	.venv/bin/alembic revision --autogenerate -m "schema update"
test:
	.venv/bin/pytest
lint:
	.venv/bin/ruff check apps/api && .venv/bin/mypy apps/api/flowapi
format:
	.venv/bin/ruff format apps/api && .venv/bin/ruff check --fix apps/api
demo:
	docker compose exec -T api python -m flowapi.demo
clean:
	docker compose down
