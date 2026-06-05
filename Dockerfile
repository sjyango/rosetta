FROM python:3.11-slim

LABEL description="Rosetta SQL Playground for With Platform"
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

# Environment variables for MySQL (history/favorites storage)
ENV MYSQL_HOST=11.142.154.110
ENV MYSQL_PORT=3306
ENV MYSQL_USER=with_ugmatclusdrxhadd
ENV MYSQL_PASSWORD=Xe#RGXP8$a0XQQ
ENV MYSQL_DATABASE=rf5otpny

# Default config path (With platform 专用配置)
ENV ROSETTA_CONFIG=/app/with_config.json

# Run playground server
CMD ["python", "-m", "rosetta.playground_server", "--config", "/app/with_config.json", "--port", "19527", "--output-dir", "/app/results"]
