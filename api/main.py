from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from api.routers.score import router as score_router

app = FastAPI(
    title="Health Inspection Coach — Ahmedabad",
    version="1.0.0",
    description="Ahmedabad restaurant risk intelligence platform.",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

# API Routers
app.include_router(score_router, prefix="/api")

# Serve the Next.js static export
# The 'out' directory from Next.js build will be copied to /app/frontend/out
if os.path.exists("frontend/out"):
    app.mount("/", StaticFiles(directory="frontend/out", html=True), name="frontend")
