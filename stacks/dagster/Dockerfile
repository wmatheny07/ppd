FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libpq-dev \
    postgresql-client \
    curl \
    tesseract-ocr \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

ENV SHELL=/bin/bash
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update \
    && ln -s /root/.local/bin/dbt /usr/local/bin/dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY project/ .
RUN pip install --no-cache-dir -e .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV DAGSTER_HOME=/opt/dagster/dagster_home

EXPOSE 4000
ENTRYPOINT ["/entrypoint.sh"]
