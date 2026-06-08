# Multi-stage build for Rosetta Playground
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies stage — install only production deps
FROM base AS deps
COPY pyproject.toml .
RUN pip install --no-cache-dir pymysql rich prompt_toolkit

# Runner stage
FROM base AS runner

# Create non-root user
RUN useradd -m -u 1001 rosetta

# Copy installed packages from deps stage
COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application source
COPY --chown=rosetta:rosetta rosetta/ ./rosetta/
COPY --chown=rosetta:rosetta pyproject.toml .
COPY --chown=rosetta:rosetta with_config.json .

# MySQL persistence environment variables
ENV MYSQL_HOST=11.142.154.110 \
    MYSQL_PORT=3306 \
    MYSQL_USER=with_ugmatclusdrxhadd \
    MYSQL_PASSWORD=Xe#RGXP8$a0XQQ \
    MYSQL_DATABASE=rf5otpny

USER rosetta

EXPOSE 8000

CMD ["python3", "-m", "rosetta.playground_server", "--config", "with_config.json", "--port", "8000", "--output-dir", "/app/results"]
