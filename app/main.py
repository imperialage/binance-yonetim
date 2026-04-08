"""Market Intelligence Service – FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from app.config import settings
from app.modules.daily_metrics_updater import start_daily_metrics_updater, stop_daily_metrics_updater
from app.modules.ha_signal_engine import start_ha_engine_loop, stop_ha_engine_loop
from app.modules.trailing_tp import start_trailing_tp, stop_trailing_tp
from app.modules.price_stream import start_price_stream, stop_price_stream
from app.modules.redis_client import close_redis
from app.modules.binance_client import close_client as close_binance
from app.modules.signal_store import close_db, init_db
from app.modules.trade_store import init_trade_db, close_trade_db
from app.modules.candle_store import init_candle_db, close_candle_db
from app.modules.scheduler import start_scheduler, stop_scheduler
from app.modules.data_collector import start_default_collections, stop_all_collections
from app.modules.st_signal_logger import init_st_signal_db, close_st_signal_db
from app.modules.st_stats_updater import start_st_stats_updater, stop_st_stats_updater
from app.modules.indicator_settings_store import init_indicator_settings_db, close_indicator_settings_db
from app.modules.order_stream import start_order_stream, stop_order_stream
from app.modules.signal_engine import start_signal_engines, stop_signal_engines
from app.routers import admin, backtest, chart, data_collector, events, latest, status, webhook, ws
from app.routers import st_webhook, indicator_settings, strategy_report
from app.utils.logging import get_logger, setup_logging

setup_logging(log_level=settings.log_level, json_output=settings.log_json)
log = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    import os
    data_dir = os.getenv("DATA_DIR", "data")
    os.makedirs(data_dir, exist_ok=True)
    log.info("startup", env=settings.app_env, data_dir=os.path.abspath(data_dir))
    await init_db()
    await init_trade_db()
    await init_candle_db()
    await init_st_signal_db()
    await init_indicator_settings_db()
    start_scheduler()
    start_st_stats_updater()
    start_price_stream()
    start_order_stream()
    start_signal_engines()
    start_ha_engine_loop()
    start_default_collections()
    start_daily_metrics_updater()
    start_trailing_tp()
    yield
    await stop_trailing_tp()
    await stop_daily_metrics_updater()
    await stop_ha_engine_loop()
    await stop_signal_engines()
    await stop_order_stream()
    await stop_price_stream()
    await stop_all_collections()
    await stop_st_stats_updater()
    await stop_scheduler()
    await close_binance()
    await close_redis()
    await close_indicator_settings_db()
    await close_st_signal_db()
    await close_candle_db()
    await close_trade_db()
    await close_db()
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
app.include_router(chart.router, tags=["chart"])
app.include_router(backtest.router, tags=["backtest"])
app.include_router(data_collector.router, tags=["data-collector"])
app.include_router(st_webhook.router, tags=["st-webhook"])
app.include_router(indicator_settings.router, tags=["indicator-settings"])
app.include_router(strategy_report.router, tags=["strategy-report"])
from app.routers import pine_sim  # noqa: E402
app.include_router(pine_sim.router, tags=["pine-sim"])

# ── Static files & page routes ────────────────────────
_static_dir = Path(__file__).resolve().parent.parent / "static"


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(str(_static_dir / "index.html"))


@app.get("/trading")
async def trading_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "trading.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/backtest")
async def backtest_page() -> FileResponse:
    return FileResponse(str(_static_dir / "backtest.html"))


@app.get("/chart")
async def chart_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "chart.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/settings")
async def settings_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "settings.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/monitor")
async def monitor_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "monitor.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/collector")
async def collector_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "data_collector.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/ha-monitor")
async def ha_monitor_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "ha_monitor.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/strategy")
async def strategy_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "strategy.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/pine-monitor")
async def pine_monitor_page() -> FileResponse:
    return FileResponse(
        str(_static_dir / "pine_monitor.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── Exception handlers ──────────────────────────────
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
