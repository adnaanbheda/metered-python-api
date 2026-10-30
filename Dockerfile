FROM python:3.13-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app/src

# Dependencies first so code edits don't bust the install layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY main.py ./
COPY src ./src
COPY scripts ./scripts
COPY mocks ./mocks


# Gate: ruff + unit tests against a throwaway Redis. Nothing from this stage
# ships except the /passed marker.
FROM base AS check

RUN apt-get update \
    && apt-get install -y --no-install-recommends redis-server \
    && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --no-install-project

COPY tests ./tests
# conftest points at 127.0.0.1:6380
RUN ruff check --no-cache . && pytest && touch /passed


FROM base

# BuildKit skips stages nothing depends on; this makes the check stage mandatory.
COPY --from=check /passed /tmp/check-passed

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
