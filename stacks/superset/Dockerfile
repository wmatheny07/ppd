FROM superset-with-pg:latest

USER root

# Install system dependencies required by Playwright's Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright Python package and its Chromium browser.
# PLAYWRIGHT_BROWSERS_PATH must point outside /app/superset_home — that
# directory is a bind-mounted volume at runtime, which would shadow anything
# installed there during the image build.
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright-browsers
RUN /app/.venv/bin/pip3 install --no-cache-dir playwright \
    && /app/.venv/bin/playwright install chromium

USER superset
