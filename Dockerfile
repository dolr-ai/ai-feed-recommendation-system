# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Install system dependencies required for building Python packages
# build-essential is needed for hiredis C extension
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Copy production entrypoint script
COPY entrypoint_production.sh .
RUN chmod +x entrypoint_production.sh

# Create a non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose the FastAPI port
EXPOSE 8000

# Health check for Fly.io
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Use the production entrypoint script
# This script handles worker count calculation and uvicorn startup
CMD ["./entrypoint_production.sh"]