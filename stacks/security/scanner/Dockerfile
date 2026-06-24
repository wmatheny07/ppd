FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    ca-certificates \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Trivy — container + config vulnerability scanner (Aqua Security)
RUN curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh \
    | sh -s -- -b /usr/local/bin

# Syft — SBOM generator (Anchore)
RUN curl -sSfL https://raw.githubusercontent.com/anchore/syft/main/install.sh \
    | sh -s -- -b /usr/local/bin

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
