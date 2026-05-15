#!/bin/bash

# Substitute the PORT environment variable into the nginx config
envsubst '${PORT}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf

# Start the FastAPI backend
echo "Starting Backend on localhost:8000..."
uvicorn api.main:app --host localhost --port 8000 > /dev/stdout 2>&1 &

# Start the Next.js frontend on internal port 3000
echo "Starting Frontend on localhost:3000..."
PORT=3000 node frontend/server.js > /dev/stdout 2>&1 &

# Start Nginx in the foreground
echo "Starting Nginx Proxy on Port $PORT..."
nginx -g 'daemon off;'
