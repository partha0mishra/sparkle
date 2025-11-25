#!/bin/bash
#
# Quick start script for Sparkle Studio Backend
#

set -e

echo "🚀 Starting Sparkle Studio Backend..."
echo ""

# Navigate to backend directory
cd "$(dirname "$0")/backend"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies if needed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "📥 Installing dependencies..."
    pip install -r requirements.txt
fi

# Set environment variables
export SPARKLE_ENV=local
export DEBUG=true
export BACKEND_CORS_ORIGINS='["http://localhost:3000", "http://localhost:8000", "http://localhost"]'
export GIT_REPO_PATH=/tmp/sparkle_git_repo
export GIT_DEFAULT_BRANCH=main
export SPARK_MASTER=local[*]
export SPARK_APP_NAME=SparkleStudio
export SECRET_KEY=dev-secret-key-change-in-production

# Spark network binding (fixes macOS issues)
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DRIVER_BIND_ADDRESS=127.0.0.1
export SPARK_DRIVER_HOST=127.0.0.1

# Python path
export PYTHONPATH=/home/user/sparkle:$PYTHONPATH

# Create and initialize git repo directory if it doesn't exist
mkdir -p "$GIT_REPO_PATH"
if [ ! -d "$GIT_REPO_PATH/.git" ]; then
    echo "📝 Initializing Git repository..."
    cd "$GIT_REPO_PATH"
    git init
    git config user.name "Sparkle Studio"
    git config user.email "studio@sparkle.local"
    echo "# Sparkle Pipelines" > README.md
    git add README.md
    git commit -m "Initial commit"
    cd -
fi

echo ""
echo "✅ Starting FastAPI server on http://localhost:8000"
echo "📚 API docs: http://localhost:8000/docs"
echo "🔍 Health check: http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start the server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
