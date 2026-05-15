from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import asyncio

from api.routers.score import router as score_router

app = FastAPI(
    title="Health Inspection Coach — Ahmedabad",
    version="1.0.0",
    description="""
Predict any Ahmedabad restaurant's FSSAI inspection risk before the inspector shows up.

**Data source:** 15 pre-seeded Ahmedabad restaurants with FSSAI license IDs.
Environmental risk factors are assessed in real-time via Google Places Nearby Search
within a 200-meter radius — checking for markets, transit hubs, and construction.

**Key endpoints:**
- `POST /score` — risk prediction for a restaurant (by FSSAI license ID)
- `GET /health` — health check
- `GET /metadata` — app version info

**Scoring:** heuristic-based. Risk is derived from base inspection history,
environmental index (nearby risk factors), and FSSAI Schedule 4 violation probabilities.

**Environmental index:** real-time composite built from Google Places Nearby Search —
counts of marketplaces, bus stations, transit stations, and construction sites
within ~200m of each restaurant.
""",
)

# Primary CORS config
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # no credentials, so wildcard is fine
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

# Belt & suspenders: ensure headers exist on ALL responses (incl. errors)
@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    origin = request.headers.get("origin")
    # If origin present and middleware didn't set headers (some error paths),
    # add permissive CORS so browsers accept the response.
    if origin and "access-control-allow-origin" not in (k.lower() for k in resp.headers.keys()):
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers.setdefault("Vary", "Origin")
        resp.headers.setdefault("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
        resp.headers.setdefault("Access-Control-Allow-Headers", "Authorization,Content-Type,Accept,Origin")
        resp.headers.setdefault("Access-Control-Max-Age", "86400")
    return resp

# Catch-all preflight so OPTIONS never 404s
@app.options("/{rest_of_path:path}")
def preflight(rest_of_path: str, request: Request):
    origin = request.headers.get("origin", "*")
    acrm = request.headers.get("access-control-request-method", "GET,POST,OPTIONS")
    acrh = request.headers.get("access-control-request-headers", "authorization,content-type")
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": origin if origin else "*",
            "Access-Control-Allow-Methods": acrm,
            "Access-Control-Allow-Headers": acrh,
            "Access-Control-Max-Age": "86400",
        },
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metadata")
def metadata():
    return {
        "model_version": "ahmedabad-v1.0",
        "city": "Ahmedabad, Gujarat, India",
        "data_source": "FSSAI demo seed + Google Places Nearby Search (live)",
        "restaurants_seeded": 15,
    }

# Routers
app.include_router(score_router, prefix="")

# Catch-all proxy for the Frontend
@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
async def proxy_frontend(path_name: str, request: Request):
    # Use 127.0.0.1 for direct loopback in Docker
    target_url = f"http://127.0.0.1:3000/{path_name}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Simple retry logic in case Next.js is still starting up
        for attempt in range(5):
            try:
                # Strip headers that cause encoding/routing issues in proxying
                req_headers = dict(request.headers)
                req_headers.pop("accept-encoding", None)
                req_headers.pop("host", None)
                req_headers.pop("connection", None)

                content = await request.body()
                proxy_req = client.build_request(
                    method=request.method,
                    url=target_url,
                    headers=req_headers,
                    content=content,
                    params=request.query_params
                )
                proxy_resp = await client.send(proxy_req, stream=True)
                
                # Sanitize headers (exclude hop-by-hop and content-length)
                excluded = ["content-encoding", "content-length", "transfer-encoding", "connection", "keep-alive"]
                headers = {k: v for k, v in proxy_resp.headers.items() if k.lower() not in excluded}
                
                return StreamingResponse(
                    proxy_resp.aiter_raw(),
                    status_code=proxy_resp.status_code,
                    headers=headers
                )
            except Exception as e:
                print(f"Proxy attempt {attempt+1} failed: {e}", flush=True)
                if attempt == 4:
                    raise
                await asyncio.sleep(2)
