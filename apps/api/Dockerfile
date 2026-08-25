FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY apps/api/flowapi apps/api/flowapi
COPY alembic.ini ./
COPY apps/api/alembic apps/api/alembic
RUN pip install --no-cache-dir .
CMD ["uvicorn", "flowapi.main:app", "--host", "0.0.0.0", "--port", "8000"]
