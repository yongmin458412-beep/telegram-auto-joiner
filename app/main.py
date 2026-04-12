"""FastAPI 앱 — 다중 계정 워커 풀."""
from __future__ import annotations

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from . import forwarder, scheduler, storage
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

    # worker=None 인 joined 그룹들을 계정 1에 임시 배정
    try:
        state_fix = storage.read_state()
        reassigned = 0
        for g in state_fix["groups"]:
            if g.get("status") == "joined" and not g.get("worker"):
                g["worker"] = "1"
                reassigned += 1
        if reassigned:
            storage.write_state(state_fix)
            log.info("Reassigned %d worker=None groups to account #1.", reassigned)
    except Exception as e:
        log.error("Worker reassign failed: %s", e)

    # 재배포 시 forward 라운드 상태 + 에러 초기화 (깨끗하게 재시작)
    try:
        state_fwd = storage.read_state()
        reset_count = 0
        for acc in state_fwd["stats"]["accounts"].values():
            acc["forward_current_round_remaining"] = []
            acc["forward_current_round_total"] = 0
            acc["forward_next_round_at"] = None
            acc["forward_rounds_today"] = 0
            acc["forward_floodwait_events"] = []
            acc["forward_pause_until"] = None
        state_fwd["stats"]["global_forward_floodwait_events"] = []
        for g in state_fwd["groups"]:
            if g.get("last_forward_error"):
                g["last_forward_error"] = None
                reset_count += 1
        storage.write_state(state_fwd)
        if reset_count:
            log.info("Reset %d forward errors + round state for fresh start.", reset_count)
    except Exception as e:
        log.error("Forward state reset failed: %s", e)

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

    # 각 계정 클라이언트 연결 시도 (병렬)
    async def _try_connect(name: str) -> tuple[str, bool, Optional[str]]:
        try:
            await get_client(name)
            log.info("[%s] Telegram client connected.", name)
            return name, True, None
        except Exception as e:
            log.error("[%s] Client connect failed: %s", name, e)
            return name, False, str(e)

    connect_results = await asyncio.gather(
        *[_try_connect(name) for name, _s in accounts],
        return_exceptions=False,
    )
    connected = [name for name, ok, _err in connect_results if ok]

    # 연결된 계정들로 소스 채널 자동 resolve (가입)
    if connected:
        try:
            log.info("Auto-resolving source channels for %d accounts...", len(connected))
            resolve_results = await forwarder.resolve_source_all(connected)
            for name, res in resolve_results.items():
                log.info("[%s] source resolve: %s", name, res)
        except Exception as e:
            log.error("Source auto-resolve failed: %s", e)

    if not connected:
        log.error("No Telegram clients connected. Workers will not start.")
    else:
        log.info(
            "Starting %d join worker(s) + %d forward worker(s). "
            "join_delay=%d-%ds rounds=%d/day interval=%sh within_round=%d-%ds",
            len(connected), len(connected),
            scheduler.JOIN_DELAY_MIN, scheduler.JOIN_DELAY_MAX,
            forwarder.DAILY_ROUND_LIMIT, forwarder.ROUND_INTERVAL_HOURS,
            forwarder.WITHIN_ROUND_DELAY_MIN, forwarder.WITHIN_ROUND_DELAY_MAX,
        )
        for name in connected:
            join_task = asyncio.create_task(scheduler.run_worker(name))
            fwd_task = asyncio.create_task(forwarder.run_forward_worker(name))
            _worker_tasks.append(join_task)
            _worker_tasks.append(fwd_task)

    yield

    scheduler.stop()
    forwarder.stop()
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
        "forward_enabled": sum(1 for g in state["groups"] if g.get("forward_enabled")),
    }

    forward_config = state.get("forward_config", {})
    fc_accounts = forward_config.get("accounts", {})

    # 발견된 계정 목록 (env var 기반 + 이미 등록된 통계 기반 합집합)
    from .telegram_client import discover_accounts as _discover_accounts
    discovered = [name for name, _ in _discover_accounts()]
    for name in state["stats"]["accounts"].keys():
        if name not in discovered:
            discovered.append(name)
    discovered.sort(key=lambda n: (0, int(n)) if n.isdigit() else (1, n))

    # 계정별 뷰
    accounts_view = []
    for name in discovered:
        acc = state["stats"]["accounts"].get(name, {})
        fc_acc = fc_accounts.get(name, {})
        accounts_view.append({
            "name": name,
            "joined_today": acc.get("joined_today", 0),
            "joined_this_hour": acc.get("joined_this_hour", 0),
            "effective_limit": acc.get("effective_daily_limit"),
            "fw_count": len(acc.get("floodwait_events", [])),
            "pause_until": acc.get("pause_until"),
            "last_activity": acc.get("last_activity"),
            "forwarded_today": acc.get("forwarded_today", 0),
            "forwarded_total": acc.get("forwarded_total", 0),
            "forward_fw_count": len(acc.get("forward_floodwait_events", [])),
            "forward_pause_until": acc.get("forward_pause_until"),
            "forward_effective_limit": acc.get("forward_effective_limit"),
            "forward_rounds_today": acc.get("forward_rounds_today", 0),
            "forward_current_round_remaining_count": len(
                acc.get("forward_current_round_remaining", []) or []
            ),
            "forward_current_round_total": acc.get("forward_current_round_total", 0),
            "forward_next_round_at": acc.get("forward_next_round_at"),
            "source_link": fc_acc.get("source_link", ""),
            "source_message_id": fc_acc.get("source_message_id", 0),
            "source_accessible": fc_acc.get("source_accessible", False),
            "source_last_check": fc_acc.get("last_check"),
            "source_last_check_error": fc_acc.get("last_check_error"),
            "direct_message": fc_acc.get("direct_message", ""),
        })

    global_fw = len(state["stats"].get("global_floodwait_events", []))
    global_forward_fw = len(state["stats"].get("global_forward_floodwait_events", []))
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
            "forward_enabled_global": forward_config.get("enabled", False),
            "forward_initial_delay_hours": forward_config.get("initial_delay_hours", 24),
            "forward_daily_limit": forwarder.DAILY_FORWARD_LIMIT,
            "forward_daily_round_limit": forwarder.DAILY_ROUND_LIMIT,
            "forward_round_interval_hours": forwarder.ROUND_INTERVAL_HOURS,
            "global_forward_fw": global_forward_fw,
            "global_forward_fw_threshold": forwarder.FORWARD_FW_PAUSE_THRESHOLD,
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


# ---------- Forward 라우트 ----------

@app.post("/forward/config/{acc_name}")
async def forward_config_account(
    acc_name: str,
    source_link: str = Form(""),
    message_id: int = Form(0),
    _: str = Depends(auth),
):
    if source_link.strip() and message_id > 0:
        storage.set_forward_config_account(acc_name, source_link.strip(), int(message_id))
        log.info("Forward config saved for account %s", acc_name)
    else:
        log.warning("Forward config rejected for account %s (empty)", acc_name)
    return RedirectResponse("/", status_code=303)


@app.post("/forward/direct/{acc_name}")
async def forward_direct_message(
    acc_name: str,
    direct_message: str = Form(""),
    _: str = Depends(auth),
):
    storage.set_forward_direct_message(acc_name, direct_message)
    log.info("Direct message saved for account %s (%d chars)", acc_name, len(direct_message.strip()))
    return RedirectResponse("/", status_code=303)


@app.post("/forward/initial-delay")
async def forward_initial_delay(hours: float = Form(...), _: str = Depends(auth)):
    storage.set_forward_initial_delay(hours)
    return RedirectResponse("/", status_code=303)


@app.post("/forward/toggle-global")
async def forward_toggle_global(enabled: str = Form(""), _: str = Depends(auth)):
    storage.set_forward_enabled_global(enabled.lower() in ("1", "true", "on", "yes"))
    return RedirectResponse("/", status_code=303)


@app.post("/forward/resolve")
async def forward_resolve(_: str = Depends(auth)):
    accounts = [n for n, _s in discover_accounts()]
    log.info("Resolving source channels for %d accounts...", len(accounts))
    results = await forwarder.resolve_source_all(accounts)
    log.info("Resolve results: %s", results)
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/forward-toggle")
async def group_forward_toggle(
    group_id: str,
    enabled: str = Form(""),
    _: str = Depends(auth),
):
    storage.toggle_forward(group_id, enabled.lower() in ("1", "true", "on", "yes"))
    return RedirectResponse("/", status_code=303)


@app.post("/groups/{group_id}/forward-reset-count")
async def group_forward_reset(group_id: str, _: str = Depends(auth)):
    storage.reset_forward_count(group_id)
    return RedirectResponse("/", status_code=303)


@app.post("/forward/enable-all-joined")
async def forward_enable_all_joined(_: str = Depends(auth)):
    count = storage.enable_forward_all_joined()
    log.info("forward_enabled turned ON for %d joined groups", count)
    return RedirectResponse("/", status_code=303)


@app.post("/forward/disable-all")
async def forward_disable_all(_: str = Depends(auth)):
    count = storage.disable_forward_all()
    log.info("forward_enabled turned OFF for %d groups", count)
    return RedirectResponse("/", status_code=303)


@app.get("/healthz")
async def healthz():
    return {"ok": True}
