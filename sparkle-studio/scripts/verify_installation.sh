#!/bin/bash
# Verification script for Sparkle Studio Phase 1 installation

set -e

echo "🔍 Verifying Sparkle Studio Phase 1 Installation..."
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if docker-compose is running
echo "1. Checking Docker Compose services..."
if docker-compose ps | grep -q "studio-backend.*Up"; then
    echo -e "${GREEN}✅ Backend service is running${NC}"
else
    echo -e "${RED}❌ Backend service is not running${NC}"
    echo "   Run: make up"
    exit 1
fi

# Check health endpoint
echo ""
echo "2. Checking health endpoint..."
if curl -s -f http://localhost:8000/health > /dev/null; then
    echo -e "${GREEN}✅ Health endpoint responding${NC}"
    HEALTH=$(curl -s http://localhost:8000/health)
    echo "   Status: $(echo $HEALTH | jq -r '.data.status')"
    echo "   Spark Available: $(echo $HEALTH | jq -r '.data.spark_available')"
else
    echo -e "${RED}❌ Health endpoint not responding${NC}"
    exit 1
fi

# Check OpenAPI docs
echo ""
echo "3. Checking OpenAPI documentation..."
if curl -s -f http://localhost:8000/openapi.json > /dev/null; then
    echo -e "${GREEN}✅ OpenAPI spec available${NC}"
    echo "   Visit: http://localhost:8000/docs"
else
    echo -e "${RED}❌ OpenAPI spec not available${NC}"
    exit 1
fi

# Check API endpoints
echo ""
echo "4. Checking API endpoints..."

# Components endpoint
if curl -s -f http://localhost:8000/api/v1/components > /dev/null; then
    echo -e "${GREEN}✅ Components API responding${NC}"
else
    echo -e "${RED}❌ Components API not responding${NC}"
fi

# Pipelines endpoint
if curl -s -f http://localhost:8000/api/v1/pipelines > /dev/null; then
    echo -e "${GREEN}✅ Pipelines API responding${NC}"
else
    echo -e "${RED}❌ Pipelines API not responding${NC}"
fi

# Git status endpoint
if curl -s -f http://localhost:8000/api/v1/git/status > /dev/null; then
    echo -e "${GREEN}✅ Git API responding${NC}"
else
    echo -e "${RED}❌ Git API not responding${NC}"
fi

# Check Spark UI
echo ""
echo "5. Checking Spark services..."
if curl -s -f http://localhost:8080 > /dev/null; then
    echo -e "${GREEN}✅ Spark Master UI available${NC}"
    echo "   Visit: http://localhost:8080"
else
    echo -e "${YELLOW}⚠️  Spark Master UI not responding${NC}"
fi

# Check directory structure
echo ""
echo "6. Checking directory structure..."
REQUIRED_DIRS=(
    "backend/api"
    "backend/core"
    "backend/schemas"
    "backend/services"
    "backend/utils"
    "config"
    "data"
)

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo -e "${GREEN}✅ $dir exists${NC}"
    else
        echo -e "${RED}❌ $dir missing${NC}"
    fi
done

# Check required files
echo ""
echo "7. Checking required files..."
REQUIRED_FILES=(
    "backend/main.py"
    "backend/requirements.txt"
    "backend/Dockerfile"
    "docker-compose.yml"
    "config/studio.yaml"
    "README.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✅ $file exists${NC}"
    else
        echo -e "${RED}❌ $file missing${NC}"
    fi
done

echo ""
echo -e "${GREEN}✨ Phase 1 verification complete!${NC}"
echo ""
echo "Quick links:"
echo "  • Backend API:  http://localhost:8000"
echo "  • API Docs:     http://localhost:8000/docs"
echo "  • Health Check: http://localhost:8000/health"
echo "  • Spark UI:     http://localhost:8080"
echo ""
echo "Next steps:"
echo "  • Test API endpoints using the docs interface"
echo "  • Create a test pipeline: curl -X POST http://localhost:8000/api/v1/pipelines?pipeline_name=test"
echo "  • View logs: make logs-backend"
echo ""
