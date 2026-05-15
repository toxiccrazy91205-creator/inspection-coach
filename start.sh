#!/bin/bash

# Start the FastAPI server (which now also serves the frontend files)
echo "Starting Unified Health Inspection Coach on Port $PORT..."
uvicorn api.main:app --host 0.0.0.0 --port $PORT
