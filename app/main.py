"""FastAPI 앱 (대시보드 + API)."""
from __future__ import annotations

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from . import scheduler, storage
from .telegram_client import disconnect_client, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

DASHBOARD_USER = os.environ.get("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
security = HTTPBasic()


def auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    if not DASHBOARD_PASSWORD:
        return "anonymous"  # auth disabled
    ok_u = secrets.compare_digest(credentials.username, DASHBOARD_USER)
    ok_p = secrets.compare_digest(credentials.password, DASHBOARD_PASSWORD)
    if not (ok_u and ok_p):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


_bg_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bg_task
    try:
        await get_client()
        log.info("Telegram client connected.")
    except Exception as e:
        log.error("Failed to connect Telegram client at startup: %s", e)
    _bg_task = asyncio.create_task(scheduler.run_joiner_loop())
    yield
    scheduler.stop()
    if _bg_task:
        try:
            await asyncio.wait_for(_bg_task, timeout=5)
        except asyncio.TimeoutError:
            _bg_task.cancel()
    await disconnect_client()


app = FastAPI(lifespan=lifespan, title="Telegram Auto Joiner")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, _: str = Depends(auth)):
    state = storage.read_state()
    groups = list(reversed(state["groups"]))
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "groups": groups,
            "stats": state["stats"],
            "daily_limit": scheduler.DAILY_LIMIT,
        },
    )


@app.post("/groups")
async def add_group(link: str = Form(...), _: str = Depends(auth)):
    link = link.strip()
    if not link:
        raise HTTPException(400, "link required")
    # 여러 줄 붙여넣기 지원
    added = 0
    for line in link.splitlines():
        line = line.strip()
        if line:
            storage.add_group(line)
            added += 1
    log.info("Added %d group(s).", added)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/delete")
async def delete_group(group_id: str, _: str = Depends(auth)):
    storage.delete_group(group_id)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/retry")
async def retry_group(group_id: str, _: str = Depends(auth)):
    storage.reset_to_pending(group_id)
    return RedirectResponse("/", status_code=303)


@app.get("/api/groups")
async def api_groups(_: str = Depends(auth)):
    return storage.read_state()


@app.get("/healthz")
async def healthz():
    return {"ok": True}
