#!/bin/bash

# Substitute the PORT environment variable into the nginx config
envsubst '${PORT}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf

# Start the FastAPI backend
echo "Starting Backend..."
uvicorn api.main:app --host 127.0.0.1 --port 8000 &

# Start the Next.js frontend on internal port 3000
echo "Starting Frontend..."
PORT=3000 node frontend/server.js &

# Start Nginx in the foreground
echo "Starting Nginx Proxy on Port $PORT..."
nginx -g 'daemon off;'
