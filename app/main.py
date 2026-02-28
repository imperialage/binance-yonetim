"""Market Intelligence Service – FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.config import settings
from app.modules.price_stream import start_price_stream, stop_price_stream
from app.modules.redis_client import close_redis
from app.modules.scheduler import start_scheduler, stop_scheduler
from app.routers import admin, events, latest, status, webhook, ws
from app.utils.logging import get_logger, setup_logging

setup_logging(log_level=settings.log_level, json_output=settings.log_json)
log = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    log.info("startup", env=settings.app_env)
    start_scheduler()
    start_price_stream()
    yield
    await stop_price_stream()
    await stop_scheduler()
    await close_redis()
    log.info("shutdown")


app = FastAPI(
    title="Market Intelligence Service",
    version="1.0.0",
    description="TradingView multi-indicator aggregator with deterministic rules engine",
    lifespan=lifespan,
)

# ── Routers ──────────────────────────────────────────
app.include_router(webhook.router, tags=["webhook"])
app.include_router(status.router, tags=["health"])
app.include_router(latest.router, tags=["evaluation"])
app.include_router(events.router, tags=["events"])
app.include_router(admin.router, tags=["admin"])
app.include_router(ws.router, tags=["websocket"])


# ── Exception handlers ──────────────────────────────
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
