"""FastAPI backend for the Personal Finance iOS app."""
import sys
import os

# Allow imports from the parent project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import dashboard, register, reports, static_data, market_data

app = FastAPI(title="Personal Finance API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router,    prefix="/api/dashboard",    tags=["dashboard"])
app.include_router(register.router,     prefix="/api/register",     tags=["register"])
app.include_router(reports.router,      prefix="/api/reports",      tags=["reports"])
app.include_router(static_data.router,  prefix="/api/static-data",  tags=["static-data"])
app.include_router(market_data.router,  prefix="/api/market-data",  tags=["market-data"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
