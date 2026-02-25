FROM python:3.10-slim

WORKDIR /app

# Playwright için sistem bağımlılıkları
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libatk-bridge2.0-0 libcups2 libxss1 \
    libgbm1 libgtk-3-0 libasound2 libx11-6 libxcb1 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libdbus-1-3 libxkbcommon0 \
    libatspi2.0-0 libdrm2 libpango-1.0-0 libcairo2 ca-certificates \
    fonts-liberation curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -m playwright install --with-deps chromium

COPY . .

# Veritabanı dosyası için kalıcı klasör
RUN mkdir -p /data
ENV DATABASE_URL=sqlite:////data/app.db

EXPOSE 8000

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
