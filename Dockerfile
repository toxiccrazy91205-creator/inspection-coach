# --- Stage 1: Build Frontend ---
FROM node:20-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
ENV NEXT_PUBLIC_API_URL=/api
RUN npm run build

# --- Stage 2: Final Runtime ---
FROM python:3.11-slim
WORKDIR /app

# Install Node.js
RUN apt-get update && apt-get install -y \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Backend dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/
COPY data/ ./data/

# Copy Frontend standalone build
COPY --from=frontend-builder /app/frontend/.next/standalone ./frontend/
COPY --from=frontend-builder /app/frontend/.next/static ./frontend/.next/static
COPY --from=frontend-builder /app/frontend/public ./frontend/public

# Setup Startup Script
COPY start.sh .
RUN sed -i 's/\r$//' start.sh && chmod +x start.sh

# Environment variables
ENV PORT=10000
ENV DEMO_SEED_FILE=./data/demo_seed.json

EXPOSE 10000

CMD ["./start.sh"]
