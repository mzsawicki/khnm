FROM python:3.14-slim

WORKDIR /app

COPY . /khnm
RUN pip install /khnm

COPY demo/ .

ENV PYTHONPATH=/app
