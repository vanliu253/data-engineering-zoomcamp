FROM python:3.13.11-slim

# copy from different docker image containing uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# create working directory
WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"

# copy from local machine to docker image current directory
COPY pyproject.toml .python-version uv.lock ./

# verify if dependencies in local are synced with environment
RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]