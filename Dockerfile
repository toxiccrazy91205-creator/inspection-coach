# --- Stage 1: Build Frontend ---
FROM node:20-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
# Set API URL to /api for production proxying
ENV NEXT_PUBLIC_API_URL=/api
RUN npm run build

# --- Stage 2: Final Runtime ---
FROM python:3.11-slim
WORKDIR /app

# Install Node.js, Nginx, and envsubst
RUN apt-get update && apt-get install -y \
    curl \
    nginx \
    gettext-base \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Copy Backend files and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/
COPY data/ ./data/

# Copy Frontend standalone build
COPY --from=frontend-builder /app/frontend/.next/standalone ./frontend/
COPY --from=frontend-builder /app/frontend/.next/static ./frontend/.next/static
COPY --from=frontend-builder /app/frontend/public ./frontend/public

# Setup Nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf.template
RUN rm /etc/nginx/sites-enabled/default

# Setup Startup Script
COPY start.sh .
RUN chmod +x start.sh

# Environment variables
ENV PORT=10000
ENV DEMO_SEED_FILE=./data/demo_seed.json

EXPOSE 10000

CMD ["./start.sh"]
