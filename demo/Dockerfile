FROM python:3.14-slim AS builder

WORKDIR /khnm

RUN pip install uv
COPY pyproject.toml /khnm/pyproject.toml
RUN uv pip compile pyproject.toml -o requirements.txt
RUN uv pip install --system --no-cache -r requirements.txt

FROM builder

WORKDIR /app

COPY . /khnm
COPY pyproject.toml /khnm/pyproject.toml
RUN uv pip install --system --no-cache --no-deps /khnm

COPY demo/ .

ENV PYTHONPATH=/app
