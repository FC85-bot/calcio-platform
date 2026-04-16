FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY apps/api /app/apps/api
COPY scripts /app/scripts
COPY .env.example /app/.env.example

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e /app/apps/api

EXPOSE 8000
