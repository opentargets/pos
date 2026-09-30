ARG BASE_IMAGE=python:3.13-slim
FROM ${BASE_IMAGE}

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git && \
    rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# Copy project files
COPY pyproject.toml README.md uv.lock ./
COPY src/ ./src/
COPY config/ ./config/

# Install project dependencies
RUN uv sync

# Add the installed packages to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Entry point for the container
ENTRYPOINT ["pos"]

# Default command (can be overridden)
CMD ["--help"]
