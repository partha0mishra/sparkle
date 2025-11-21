# Sparkle Studio

**Low-code/Pro-code hybrid UI for Project Sparkle**

Sparkle Studio is a React + FastAPI application that makes the entire Sparkle framework (connections, ingestors, transformers, ml, orchestration) visually composable while preserving 100% of the code-first power.

## Core Principles

1. **100% Parity** — Every canvas pipeline is exportable as valid sparkle/orchestration config + optional custom Python files in Git
2. **Git is Source of Truth** — Every save = commit. Promotion = PR merge
3. **No Lock-in** — "Export to Git" and "Open in IDE" buttons mandatory
4. **Auto-form Generation** — JSON Schema drives all component configuration forms
5. **Flexible Deployment** — Works with local Docker demo OR remote Databricks/EMR cluster (configurable)
6. **Zero Hard-coding** — All paths, endpoints, Git repos from .env or config

## Directory Structure

```
sparkle-studio/
├── backend/              # FastAPI backend
│   ├── api/             # API endpoints
│   │   └── v1/          # API v1 routes
│   ├── core/            # Core configuration
│   ├── schemas/         # Pydantic models
│   ├── services/        # Business logic
│   ├── utils/           # Utilities
│   ├── main.py          # FastAPI app
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/            # React + TypeScript (Phase 3)
├── shared/              # JSON schemas, types
├── config/              # Configuration files
│   └── studio.yaml      # Studio configuration
├── docker-compose.yml   # Docker orchestration
└── README.md
```

## Phase 1: Backend API Core ✅

**Status: Complete**

- ✅ FastAPI app with CORS, middleware, lifespan management
- ✅ Pipeline CRUD operations with Git integration
- ✅ Execution engine (dry-run, run, backfill)
- ✅ Git operations (status, commit, pull, PR creation)
- ✅ Full OpenAPI spec at `/docs` and `/redoc`
- ✅ Type-safe with Pydantic v2
- ✅ Docker setup with Spark support

## Phase 2: Component Registry & JSON Schema Auto-Generator ✅

**Status: Complete**

- ✅ `@config_schema` decorator for automatic schema generation
- ✅ `Field()` descriptor with validation constraints and UI hints
- ✅ Component Registry with automatic discovery on startup
- ✅ 6 example components (Salesforce, JDBC, SCD Type 2, XGBoost, Kafka, Data Quality)
- ✅ Enhanced API endpoints with search and sample data
- ✅ JSON Schema validation with field-level errors
- ✅ UI metadata (widget, group, order, placeholders)
- ✅ Fuzzy search with relevance ranking
- ✅ 100% automatic - zero hard-coded schemas

## Phase 3: Frontend Core Canvas with Git Integration ✅

**Status: Complete**

- ✅ React 18 + TypeScript with strict mode
- ✅ React Flow visual canvas with drag & drop
- ✅ Custom nodes with icons and category badges
- ✅ Auto-generated forms from JSON Schema
- ✅ Git integration (branches, commit, pull, PR)
- ✅ Zustand state management with undo/redo
- ✅ Tailwind CSS beautiful UI
- ✅ Monaco code editor for SQL/Python
- ✅ Docker multi-stage build with nginx
- ✅ Full TypeScript type safety (no `any`)
- ✅ Keyboard shortcuts (Ctrl+S, Ctrl+Z)
- ✅ Component search and filtering

### API Endpoints

#### Components (Phase 2 - Enhanced)
- `GET /api/v1/components` — All components with full config schemas
- `GET /api/v1/components/category/{category}` — Filter by category
- `GET /api/v1/components/{category}/{name}` — Component detail + schema
- `POST /api/v1/components/{category}/{name}/validate` — Validate with field errors
- `GET /api/v1/components/search?q={query}` — Fuzzy search with ranking
- `GET /api/v1/components/{category}/{name}/sample-data` — Execute for preview

#### Pipelines
- `GET /api/v1/pipelines` — List all pipelines
- `GET /api/v1/pipelines/{name}` — Get pipeline
- `POST /api/v1/pipelines/{name}` — Save/update pipeline
- `DELETE /api/v1/pipelines/{name}` — Delete pipeline
- `POST /api/v1/pipelines/{name}/export` — Export to Git

#### Execution
- `POST /api/v1/execute/dry-run` — Run with sample data
- `POST /api/v1/execute/run` — Submit pipeline
- `POST /api/v1/execute/backfill` — Backfill with date range
- `GET /api/v1/executions/{run_id}` — Get execution status

#### Git
- `GET /api/v1/git/status` — Get Git status
- `GET /api/v1/git/branches` — List branches
- `POST /api/v1/git/commit` — Commit changes
- `POST /api/v1/git/pull` — Pull from remote
- `POST /api/v1/git/create-pr` — Create pull request

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- Git

### Run with Docker

1. **Clone the repository:**
   ```bash
   cd sparkle-studio
   ```

2. **Create data directory:**
   ```bash
   mkdir -p data/git_repo
   ```

3. **Start the services:**
   ```bash
   docker-compose up -d
   ```

4. **Access the services:**
   - **Frontend UI**: http://localhost:3000
   - **Backend API**: http://localhost:8000
   - **API Docs**: http://localhost:8000/docs
   - **Health Check**: http://localhost:8000/health
   - **Spark Master UI**: http://localhost:8080
   - **Spark Worker UI**: http://localhost:8081

### Local Development

1. **Set up Python environment:**
   ```bash
   cd backend
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Create `.env` file:**
   ```bash
   cp .env.example .env
   ```

3. **Run the server:**
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

## Configuration

### Environment Variables (`.env`)

See `backend/.env.example` for all available environment variables.

Key variables:
- `SPARKLE_ENV`: Environment (local/dev/prod)
- `GIT_REPO_PATH`: Path to Git repository
- `SPARK_MASTER`: Spark master URL
- `CLUSTER_TYPE`: Cluster type (local/databricks/emr)

### Studio Configuration (`config/studio.yaml`)

Configure Git strategy, cluster settings, pipeline defaults, and UI preferences.

## Development Phases

- [x] **Phase 1**: Backend API Core (Complete)
- [x] **Phase 2**: Component Registry & JSON Schema Auto-generator (Complete)
- [x] **Phase 3**: Frontend Core Canvas with Git Integration (Complete)
- [ ] **Phase 4**: Pipeline Execution & Real-time Logs
- [ ] **Phase 5**: Live Preview Engine (1000-row sample runs)
- [ ] **Phase 6**: Execution Bridge (Run/Backfill/Schedule)
- [ ] **Phase 7**: Observability Dashboard per Pipeline
- [ ] **Phase 8**: Promotion Workflow + Simple RBAC
- [ ] **Phase 9**: Demo Templates + Onboarding Flow

## Testing

### Health Check
```bash
curl http://localhost:8000/health
```

### List Components
```bash
curl http://localhost:8000/api/v1/components
```

### Create Pipeline
```bash
curl -X POST http://localhost:8000/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "test_pipeline"}'
```

## Architecture

### Backend Stack
- **Framework**: FastAPI
- **Validation**: Pydantic v2
- **Spark**: PySpark 3.5.0 + Delta Lake
- **Git**: GitPython
- **Server**: Uvicorn with async support

### Key Components

1. **Component Service**: Scans Sparkle packages, generates JSON Schema
2. **Pipeline Service**: Manages pipeline CRUD, Git integration
3. **Spark Service**: Handles execution (local & remote)
4. **Git Service**: Git operations with GitHub/GitLab API support

## License

Apache License 2.0

## Next Steps

**Phase 4** will focus on:
- Real-time pipeline execution with WebSocket logs
- Live execution monitoring and debugging
- Pipeline run history and status tracking
- Error handling and retry mechanisms
- Performance metrics and profiling

---

**Sparkle Studio** — Making data magic visible ✨
