FROM apache/superset:latest
USER root
RUN . /app/.venv/bin/activate && uv pip install --no-cache-dir snowflake-sqlalchemy
USER superset