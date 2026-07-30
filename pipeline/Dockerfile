FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version README.md ./
COPY src/ src/

RUN uv sync --locked

ENTRYPOINT [ "python", "src/pipeline/ingest_data.py" ]
