# Sparkle Studio - Quick Start Guide

Get up and running with Sparkle Studio in 5 minutes.

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM minimum
- Git

## Installation

### 1. Navigate to the project

```bash
cd sparkle-studio
```

### 2. Initialize the environment

```bash
make init
```

This creates necessary directories and copies configuration files.

### 3. Start all services

```bash
make up
```

This starts:
- Sparkle Studio Backend (port 8000)
- Apache Spark Master (port 8080, 7077)
- Apache Spark Worker (port 8081)

### 4. Verify installation

```bash
./scripts/verify_installation.sh
```

Or manually check:
```bash
curl http://localhost:8000/health | jq
```

## Usage

### Access the API

- **API Base**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

### Example Requests

#### 1. List all components

```bash
curl http://localhost:8000/api/v1/components | jq
```

#### 2. Get components by category

```bash
curl http://localhost:8000/api/v1/components/categories | jq
```

#### 3. Create a new pipeline

```bash
curl -X POST 'http://localhost:8000/api/v1/pipelines?pipeline_name=my_first_pipeline' \
  -H 'Content-Type: application/json' | jq
```

#### 4. Get pipeline details

```bash
curl http://localhost:8000/api/v1/pipelines/my_first_pipeline | jq
```

#### 5. Save a pipeline with nodes and edges

```bash
curl -X POST http://localhost:8000/api/v1/pipelines/my_first_pipeline \
  -H 'Content-Type: application/json' \
  -d '{
    "pipeline": {
      "metadata": {
        "name": "my_first_pipeline",
        "display_name": "My First Pipeline",
        "description": "A simple test pipeline"
      },
      "nodes": [
        {
          "id": "node_1",
          "type": "sparkle_component",
          "position": {"x": 100, "y": 100},
          "data": {
            "component_type": "ingestor",
            "component_name": "csv_ingestor",
            "label": "Load CSV",
            "config": {
              "path": "/data/input.csv"
            }
          }
        }
      ],
      "edges": [],
      "config": {
        "parallelism": 4
      }
    },
    "commit_message": "Initial pipeline setup"
  }' | jq
```

#### 6. Check Git status

```bash
curl http://localhost:8000/api/v1/git/status | jq
```

#### 7. Get component detail

```bash
curl http://localhost:8000/api/v1/components/ingestors/csv_ingestor | jq
```

## Development

### View logs

```bash
# All services
make logs

# Backend only
make logs-backend

# Spark only
make logs-spark
```

### Restart services

```bash
make restart
```

### Stop services

```bash
make down
```

### Clean up everything

```bash
make clean
```

⚠️ This removes all containers, volumes, and data!

### Run backend locally (without Docker)

```bash
# Install dependencies
make install-backend

# Set up environment
cd backend
cp .env.example .env

# Run server
make dev-backend
```

## Troubleshooting

### Port already in use

If port 8000 or 8080 is already in use:

```bash
# Check what's using the port
lsof -i :8000
lsof -i :8080

# Stop conflicting services or change ports in docker-compose.yml
```

### Spark not available

Check Spark services:

```bash
docker-compose ps
docker-compose logs spark-master
docker-compose logs spark-worker
```

### Git repository issues

Initialize Git repository manually:

```bash
mkdir -p data/git_repo
cd data/git_repo
git init
cd ../..
```

### Backend not starting

Check logs:

```bash
docker-compose logs studio-backend
```

Common issues:
- Missing Sparkle engine at `/opt/sparkle` (check volume mount)
- Port conflicts
- Insufficient memory

## Project Structure

```
sparkle-studio/
├── backend/              # FastAPI backend
│   ├── api/v1/          # API endpoints
│   ├── core/            # Config, dependencies, security
│   ├── schemas/         # Pydantic models
│   ├── services/        # Business logic
│   ├── utils/           # Utilities
│   ├── main.py          # FastAPI app
│   └── Dockerfile
├── config/              # Configuration
│   └── studio.yaml
├── data/                # Data directory (git_repo)
├── scripts/             # Utility scripts
├── docker-compose.yml   # Docker orchestration
├── Makefile            # Development commands
└── README.md
```

## Next Steps

1. **Explore the API**: Visit http://localhost:8000/docs
2. **Create pipelines**: Use the interactive docs to create and manage pipelines
3. **Test Git integration**: Commit and push pipeline changes
4. **Monitor Spark**: Visit http://localhost:8080 to see Spark Master UI
5. **Read documentation**: Check README.md for detailed information

## Support

- **Documentation**: See README.md and PHASE_1_COMPLETE.md
- **API Reference**: http://localhost:8000/docs
- **Issues**: Create an issue in the project repository

---

**Sparkle Studio** — Making data magic visible ✨
