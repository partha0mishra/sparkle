# Makefile for Sparkle Studio

.PHONY: help build up down restart logs clean test

help:
	@echo "Sparkle Studio - Development Commands"
	@echo ""
	@echo "Available targets:"
	@echo "  build      - Build all Docker images"
	@echo "  up         - Start all services"
	@echo "  down       - Stop all services"
	@echo "  restart    - Restart all services"
	@echo "  logs       - View logs (use 'make logs-backend' for specific service)"
	@echo "  clean      - Remove all containers, volumes, and images"
	@echo "  test       - Run backend tests"
	@echo "  init       - Initialize data directories and config"
	@echo ""

init:
	@echo "Initializing Sparkle Studio..."
	@mkdir -p data/git_repo
	@mkdir -p config
	@test -f backend/.env || cp backend/.env.example backend/.env
	@echo "✅ Initialization complete"

build:
	@echo "Building Docker images..."
	docker-compose build

up: init
	@echo "Starting Sparkle Studio..."
	docker-compose up -d
	@echo "✅ Services started"
	@echo "   Backend API: http://localhost:8000"
	@echo "   API Docs: http://localhost:8000/docs"
	@echo "   Spark UI: http://localhost:8080"

down:
	@echo "Stopping Sparkle Studio..."
	docker-compose down
	@echo "✅ Services stopped"

restart:
	@echo "Restarting Sparkle Studio..."
	docker-compose restart
	@echo "✅ Services restarted"

logs:
	docker-compose logs -f

logs-backend:
	docker-compose logs -f studio-backend

logs-spark:
	docker-compose logs -f spark-master spark-worker

clean:
	@echo "⚠️  Warning: This will remove all containers, volumes, and images"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi all; \
		rm -rf data/; \
		echo "✅ Cleanup complete"; \
	else \
		echo "Cancelled"; \
	fi

test:
	@echo "Running backend tests..."
	cd backend && python -m pytest tests/ -v

health:
	@echo "Checking service health..."
	@curl -s http://localhost:8000/health | python -m json.tool

dev-backend:
	@echo "Starting backend in development mode..."
	cd backend && uvicorn main:app --reload --host 0.0.0.0 --port 8000

install-backend:
	@echo "Installing backend dependencies..."
	cd backend && pip install -r requirements.txt

format:
	@echo "Formatting backend code..."
	cd backend && black . && isort .

lint:
	@echo "Linting backend code..."
	cd backend && flake8 . && mypy .
