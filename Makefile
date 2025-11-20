.PHONY: help test test-unit test-integration test-coverage demo-up demo-down demo-logs demo-shell clean install lint format

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Sparkle Framework - Make Commands"
	@echo "=================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Testing Commands
test: ## Run full test suite with coverage
	pytest --cov=sparkle --cov-report=html --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/test_connections.py tests/test_ingestors.py tests/test_transformers.py tests/test_ml.py tests/test_orchestration.py -v

test-integration: ## Run integration tests only
	pytest tests/test_integration.py -v

test-coverage: ## Generate coverage report
	pytest --cov=sparkle --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/index.html"

test-watch: ## Run tests in watch mode
	pytest-watch

# Demo Environment Commands
demo-up: ## Start full demo environment (Docker Compose)
	@echo "Starting Sparkle demo environment..."
	docker-compose up --build -d
	@echo ""
	@echo "✓ Demo environment started!"
	@echo ""
	@echo "Services:"
	@echo "  • Spark Master UI:     http://localhost:8080"
	@echo "  • Streamlit Dashboard: http://localhost:8501"
	@echo "  • MinIO Console:       http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  • PostgreSQL:          localhost:5432 (sparkle/sparkle123)"
	@echo "  • Kafka:               localhost:9092"
	@echo "  • Redis:               localhost:6379"
	@echo ""
	@echo "Run 'make demo-logs' to view logs"
	@echo "Run 'make demo-down' to stop all services"

demo-down: ## Stop demo environment
	@echo "Stopping Sparkle demo environment..."
	docker-compose down
	@echo "✓ Demo environment stopped"

demo-restart: ## Restart demo environment
	@echo "Restarting Sparkle demo environment..."
	docker-compose restart
	@echo "✓ Demo environment restarted"

demo-logs: ## Tail logs from all demo services
	docker-compose logs -f

demo-logs-streamlit: ## Tail Streamlit dashboard logs
	docker-compose logs -f streamlit-demo

demo-logs-spark: ## Tail Spark logs
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

demo-shell: ## Open bash shell in Spark container
	docker-compose exec spark-master bash

demo-spark-submit: ## Run the full demo pipeline with spark-submit
	docker-compose run --rm spark-submit

demo-clean: ## Clean demo data and volumes
	docker-compose down -v
	rm -rf htmlcov/ .coverage .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	@echo "✓ Demo data cleaned"

# Database Commands
db-shell: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U sparkle -d sparkle_crm

db-query: ## Run SQL query (usage: make db-query SQL="SELECT COUNT(*) FROM customers;")
	docker-compose exec postgres psql -U sparkle -d sparkle_crm -c "$(SQL)"

# Kafka Commands
kafka-topics: ## List Kafka topics
	docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-create-topic: ## Create Kafka topic (usage: make kafka-create-topic TOPIC=my-topic)
	docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic $(TOPIC) --partitions 3 --replication-factor 1

kafka-consume: ## Consume messages from topic (usage: make kafka-consume TOPIC=customer-events)
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning

# Development Commands
install: ## Install Python dependencies
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

install-hooks: ## Install pre-commit hooks
	pre-commit install

lint: ## Run code linters
	black --check .
	flake8 .
	isort --check-only .
	mypy .

format: ## Format code with black and isort
	black .
	isort .

clean: ## Clean build artifacts and caches
	rm -rf build/ dist/ *.egg-info
	rm -rf htmlcov/ .coverage .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".DS_Store" -delete

# Documentation Commands
docs: ## Build documentation
	cd docs && make html
	@echo "Documentation built in docs/_build/html/index.html"

docs-serve: ## Serve documentation locally
	cd docs/_build/html && python -m http.server 8000

# Build Commands
build: ## Build Docker images
	docker-compose build

build-no-cache: ## Build Docker images without cache
	docker-compose build --no-cache

# Status Commands
status: ## Show status of demo services
	docker-compose ps

ps: status ## Alias for status

# Benchmark Commands
benchmark: ## Run performance benchmarks
	pytest tests/benchmarks/ -v --benchmark-only

# Quick Start Command
quickstart: demo-up ## Quick start: Build and run everything
	@echo ""
	@echo "═══════════════════════════════════════════════════"
	@echo "  Sparkle Demo Environment - Quick Start Complete"
	@echo "═══════════════════════════════════════════════════"
	@echo ""
	@echo "Wait 30 seconds for all services to initialize, then:"
	@echo ""
	@echo "  1. Open Streamlit Dashboard: http://localhost:8501"
	@echo "  2. View Spark UI:             http://localhost:8080"
	@echo "  3. Access MinIO Console:      http://localhost:9001"
	@echo ""
	@echo "To run the full pipeline:"
	@echo "  make demo-spark-submit"
	@echo ""
	@echo "To stop:"
	@echo "  make demo-down"
	@echo ""
