#!/bin/bash

# Force Next.js to bind to 0.0.0.0 so loopback works correctly
echo "Starting Next.js Frontend on 0.0.0.0:3000..."
HOSTNAME=0.0.0.0 PORT=3000 node frontend/server.js > /dev/stdout 2>&1 &

# Start the FastAPI backend on the Render assigned PORT
echo "Starting FastAPI Gateway on Port $PORT..."
uvicorn api.main:app --host 0.0.0.0 --port $PORT
