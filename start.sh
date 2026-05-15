#!/bin/bash

# Start the Next.js frontend on internal port 3000
echo "Starting Next.js Frontend on localhost:3000..."
PORT=3000 node frontend/server.js > /dev/stdout 2>&1 &

# Start the FastAPI backend on the Render assigned PORT
echo "Starting FastAPI Gateway on Port $PORT..."
uvicorn api.main:app --host 0.0.0.0 --port $PORT
