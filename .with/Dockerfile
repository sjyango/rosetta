FROM python:3.11-slim

LABEL description="Rosetta SQL Playground (本地部署模式 — With 平台已弃用)"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install only runtime system deps (no build-essential, no gcc)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for caching
COPY requirements.txt pyproject.toml ./

# Install Python deps from pre-built wheels only (fast)
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY rosetta/ ./rosetta/
COPY scripts/ ./scripts/
COPY with_config.json .
COPY playground.html .

# Make scripts executable
RUN chmod +x /app/scripts/*.sh

# Create non-root user
RUN useradd -m -u 1001 rosetta && \
    mkdir -p /app/results && \
    chown -R rosetta:rosetta /app

USER rosetta

EXPOSE 8000

ENV PYTHONCORE=0
ENV SQLITE_PATH=/app/playground.db

CMD ["python3", "-m", "rosetta.playground_server", "--config", "with_config.json", "--port", "8000", "--output-dir", "/app/results"]
