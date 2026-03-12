# TikTok scraper - Playwright Python image with Chromium pre-installed
FROM mcr.microsoft.com/playwright/python:v1.58.0-noble

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py scraper.py input.json input_recipes.json ./

# Create directories the scraper needs
RUN mkdir -p images queue

EXPOSE 8080

ENV SCRAPER_SERVER_PORT=8080
ENV TIKTOK_HEADLESS=true

# --timeout 0 for long-running scrapes (OpenClaw, downloads)
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8080", "--timeout", "0", "server:app"]
