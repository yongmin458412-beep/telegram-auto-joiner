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
        return "anonymous"
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
    # 데이터 저장소 현재 상태 로깅
    try:
        log.info("DATA_DIR=%s (abs=%s)", storage.DATA_DIR, storage.DATA_DIR.resolve())
        log.info("DATA_FILE=%s exists=%s", storage.DATA_FILE, storage.DATA_FILE.exists())
        state_pre = storage.read_state()
        pre_counts = {
            "total": len(state_pre["groups"]),
            "pending": sum(1 for g in state_pre["groups"] if g["status"] == "pending"),
            "joined": sum(1 for g in state_pre["groups"] if g["status"] == "joined"),
            "failed": sum(1 for g in state_pre["groups"] if g["status"] == "failed"),
        }
        log.info("Startup state: %s", pre_counts)
    except Exception as e:
        log.error("State inspect failed: %s", e)

    # 시드 자동 주입 (groups가 비어있을 때만)
    try:
        added = storage.seed_if_empty()
        if added:
            log.info("Seeded %d groups from data/seed_links.txt", added)
        else:
            log.info("Seed skipped (groups already exist).")
    except Exception as e:
        log.error("Seed loading failed: %s", e)

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
    # 상태별 개수
    groups = list(reversed(state["groups"]))  # 최신 추가 위로
    counts = {
        "pending": sum(1 for g in state["groups"] if g["status"] == "pending"),
        "joined": sum(1 for g in state["groups"] if g["status"] == "joined"),
        "failed": sum(1 for g in state["groups"] if g["status"] == "failed"),
        "total": len(state["groups"]),
    }
    fw_count = len(state["stats"].get("floodwait_events", []))
    eff_limit = state["stats"].get("effective_daily_limit") or scheduler.DAILY_JOIN_LIMIT
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "groups": groups[:200],  # 한번에 200개만 렌더 (성능)
            "total_groups": counts["total"],
            "counts": counts,
            "stats": state["stats"],
            "daily_limit": scheduler.DAILY_JOIN_LIMIT,
            "effective_limit": eff_limit,
            "hourly_limit": scheduler.HOURLY_JOIN_LIMIT,
            "quiet_hours": scheduler.QUIET_HOURS_KST,
            "floodwait_count": fw_count,
            "pause_until": state["stats"].get("pause_until"),
        },
    )


@app.post("/groups")
async def add_group_route(link: str = Form(...), _: str = Depends(auth)):
    link = link.strip()
    if not link:
        raise HTTPException(400, "link required")
    lines = [l.strip() for l in link.splitlines() if l.strip()]
    added, skipped = storage.add_groups_bulk(lines)
    log.info("Added %d, skipped %d (duplicates/invalid).", added, skipped)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/import-seed")
async def import_seed_route(_: str = Depends(auth)):
    added, skipped = storage.import_seed_merge()
    log.info("Import seed: added=%d skipped=%d", added, skipped)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/delete")
async def delete_group_route(group_id: str, _: str = Depends(auth)):
    storage.delete_group(group_id)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/retry")
async def retry_group_route(group_id: str, _: str = Depends(auth)):
    storage.reset_to_pending(group_id)
    return RedirectResponse("/", status_code=303)


@app.get("/api/groups")
async def api_groups(_: str = Depends(auth)):
    return storage.read_state()


@app.get("/healthz")
async def healthz():
    return {"ok": True}
