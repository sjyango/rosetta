FROM python:3.11-slim

LABEL description="Rosetta SQL Playground (本地部署模式)"
LABEL version="1.0"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create results directory
RUN mkdir -p /app/results

# Expose default port
EXPOSE 19527

# SQLite 数据库路径（用于存储 history/favorites/custom DBMS）
ENV SQLITE_PATH=/app/playground.db

# Default config path
ENV ROSETTA_CONFIG=/app/with_config.json

# Run playground server
CMD ["python", "-m", "rosetta.playground_server", "--config", "/app/with_config.json", "--port", "19527", "--output-dir", "/app/results"]
