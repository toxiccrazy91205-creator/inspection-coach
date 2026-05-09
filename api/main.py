# api/main.py
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.routers.score import router as score_router
from api.routers.search import router as search_router
from api.routers.admin import router as admin_router
from api.routers.neighborhood import router as neighborhood_router
from api.routers.insights import router as insights_router

app = FastAPI(
    title="DineSafe NYC",
    version="0.1.0",
    description="""
Predict any NYC restaurant's next health inspection risk before the inspector shows up.

**Data source:** NYC Department of Health restaurant inspection records via NYC Open Data,
refreshed nightly. ~30,000 restaurants, ~296,000 inspection records.

**Key endpoints:**
- `POST /score` — risk prediction for a restaurant (by CAMIS)
- `GET /search` — find restaurants by name
- `GET /neighborhood` — rank all restaurants in a zip code by inspection risk
- `GET /insights` — citywide grade distribution and top-5 riskiest restaurants
- `GET /metadata` — data freshness info

**Scoring:** heuristic-based (not ML-trained). Risk is derived from inspection history,
score trend, borough baseline, critical violation rate, and local rodent pressure index.

**Rat pressure index:** geospatial composite built from 311 rodent complaints and DOHMH
rat inspection failures within ~150–200m of each restaurant, using H3 hex cells (resolution 9).
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
        "model_version": "0.1.0",
        "data_window_days": "1095",
        "source": "NYC Open Data (inspections), nightly ETL",
    }

# Routers
app.include_router(score_router, prefix="")
app.include_router(search_router, prefix="")
app.include_router(admin_router, prefix="")
app.include_router(neighborhood_router, prefix="")
app.include_router(insights_router, prefix="")
