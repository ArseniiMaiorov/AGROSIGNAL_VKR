FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml ./pyproject.toml
COPY scripts ./scripts
COPY src ./src
COPY tests ./tests

CMD ["python", "scripts/start_api.py"]
