FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ---- Dependencies ----
FROM base AS deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Production ----
FROM base AS runner

# Create non-root user
RUN useradd -m -u 1001 rosetta

# Copy installed packages from deps
COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application
COPY --chown=rosetta:rosetta . .

# Create output directory
RUN mkdir -p /app/output && chown rosetta:rosetta /app/output

USER rosetta

EXPOSE 8000

ENV MYSQL_HOST=11.142.154.110 \
    MYSQL_PORT=3306 \
    MYSQL_USER=with_ugmatclusdrxhadd \
    MYSQL_PASSWORD=Xe#RGXP8$a0XQQ \
    MYSQL_DATABASE=rf5otpny \
    ROSETTA_CONFIG=/app/with_config.json

CMD ["python", "-m", "rosetta.playground_server", "--config", "/app/with_config.json", "--port", "8000", "--output-dir", "/app/output"]
