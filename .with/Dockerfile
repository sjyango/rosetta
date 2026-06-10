FROM python:3.11-slim

LABEL description="Rosetta SQL Playground for With Platform"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install only runtime system deps (no build-essential, no gcc)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
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

# MySQL persistence envs
ENV MYSQL_HOST=11.142.154.110 \
    MYSQL_PORT=3306 \
    MYSQL_USER=with_ugmatclusdrxhadd \
    MYSQL_PASSWORD=Xe#RGXP8$a0XQQ \
    MYSQL_DATABASE=rf5otpny \
    ROSETTA_CONFIG=/app/with_config.json

USER rosetta

EXPOSE 8000

# Set core dump limit via env (no shell needed), then launch server directly.
# Avoid sh -c wrapper so that special characters in env values (e.g. '#' in MYSQL_PASSWORD)
# are handled correctly by Python's os.environ.
ENV PYTHONCORE=0

CMD ["python3", "-m", "rosetta.playground_server", "--config", "with_config.json", "--port", "8000", "--output-dir", "/app/results"]
