"""Shared FastAPI dependencies."""

from __future__ import annotations

from fastapi import Header, HTTPException

from app.config import settings


async def require_admin(x_admin_token: str = Header(...)) -> str:
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")
    return x_admin_token
