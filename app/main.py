"""FastAPI 앱 — 다중 계정 워커 풀."""
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
from .telegram_client import disconnect_all, discover_accounts, get_client

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


_worker_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_tasks

    # 저장소 진단 로그
    try:
        log.info("DATA_DIR=%s (abs=%s)", storage.DATA_DIR, storage.DATA_DIR.resolve())
        log.info("DATA_FILE=%s exists=%s", storage.DATA_FILE, storage.DATA_FILE.exists())
        log.info("SEED_FILE=%s exists=%s", storage.SEED_FILE, storage.SEED_FILE.exists())
        state_pre = storage.read_state()
        counts = {
            "total": len(state_pre["groups"]),
            "pending": sum(1 for g in state_pre["groups"] if g["status"] == "pending"),
            "processing": sum(1 for g in state_pre["groups"] if g["status"] == "processing"),
            "joined": sum(1 for g in state_pre["groups"] if g["status"] == "joined"),
            "failed": sum(1 for g in state_pre["groups"] if g["status"] == "failed"),
        }
        log.info("Startup state: %s", counts)
    except Exception as e:
        log.error("State inspect failed: %s", e)

    # stale processing → pending 으로 되돌림
    try:
        released = storage.release_stale_claims()
        if released:
            log.info("Released %d stale processing claims → pending.", released)
    except Exception as e:
        log.error("Release stale failed: %s", e)

    # 시드 자동 주입
    try:
        state_now = storage.read_state()
        if state_now["groups"]:
            log.info("Seed skipped: %d groups already present.", len(state_now["groups"]))
        else:
            links = storage.load_seed_links()
            log.info("Seed file loaded: %d unique links.", len(links))
            if links:
                added, _ = storage.add_groups_bulk(links)
                log.info("Seeded %d groups into groups.json.", added)
            else:
                log.warning("Seed file empty or missing: %s", storage.SEED_FILE)
    except Exception as e:
        log.error("Seed loading failed: %s", e)

    # 계정 발견
    accounts = discover_accounts()
    log.info("Discovered %d account(s): %s", len(accounts), [a[0] for a in accounts])

    # 각 계정 클라이언트 연결 시도
    connected = []
    for name, _session in accounts:
        try:
            await get_client(name)
            log.info("[%s] Telegram client connected.", name)
            connected.append(name)
        except Exception as e:
            log.error("[%s] Client connect failed: %s", name, e)

    if not connected:
        log.error("No Telegram clients connected. Workers will not start.")
    else:
        log.info(
            "Starting %d worker(s). delay=%d-%ds, start_jitter=0-%ds",
            len(connected),
            scheduler.JOIN_DELAY_MIN,
            scheduler.JOIN_DELAY_MAX,
            scheduler.WORKER_START_JITTER_MAX,
        )
        for name in connected:
            task = asyncio.create_task(scheduler.run_worker(name))
            _worker_tasks.append(task)

    yield

    scheduler.stop()
    for task in _worker_tasks:
        try:
            await asyncio.wait_for(task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            task.cancel()
    await disconnect_all()


app = FastAPI(lifespan=lifespan, title="Telegram Auto Joiner")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, _: str = Depends(auth)):
    state = storage.read_state()
    groups = list(reversed(state["groups"]))
    counts = {
        "pending": sum(1 for g in state["groups"] if g["status"] == "pending"),
        "processing": sum(1 for g in state["groups"] if g["status"] == "processing"),
        "joined": sum(1 for g in state["groups"] if g["status"] == "joined"),
        "failed": sum(1 for g in state["groups"] if g["status"] == "failed"),
        "total": len(state["groups"]),
    }
    # 계정별 유효 한도 계산
    accounts_view = []
    for name, acc in state["stats"]["accounts"].items():
        eff = acc.get("effective_daily_limit")
        accounts_view.append({
            "name": name,
            "joined_today": acc.get("joined_today", 0),
            "joined_this_hour": acc.get("joined_this_hour", 0),
            "effective_limit": eff,
            "fw_count": len(acc.get("floodwait_events", [])),
            "pause_until": acc.get("pause_until"),
            "last_activity": acc.get("last_activity"),
        })
    accounts_view.sort(key=lambda a: a["name"])
    global_fw = len(state["stats"].get("global_floodwait_events", []))
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "groups": groups[:200],
            "total_groups": counts["total"],
            "counts": counts,
            "accounts": accounts_view,
            "daily_limit": scheduler.DAILY_JOIN_LIMIT,
            "hourly_limit": scheduler.HOURLY_JOIN_LIMIT,
            "quiet_hours": scheduler.QUIET_HOURS_KST,
            "global_fw": global_fw,
            "global_fw_threshold": scheduler.GLOBAL_FW_PAUSE_THRESHOLD,
        },
    )


@app.post("/groups")
async def add_group_route(link: str = Form(...), _: str = Depends(auth)):
    link = link.strip()
    if not link:
        raise HTTPException(400, "link required")
    lines = [l.strip() for l in link.splitlines() if l.strip()]
    added, skipped = storage.add_groups_bulk(lines)
    log.info("Added %d, skipped %d.", added, skipped)
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
