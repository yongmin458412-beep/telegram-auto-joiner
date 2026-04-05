"""주기 입장 스케줄러."""
from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timezone

from . import storage
from .telegram_client import join_group

log = logging.getLogger("scheduler")

DAILY_LIMIT = int(os.environ.get("DAILY_JOIN_LIMIT", "20"))
DELAY_MIN = int(os.environ.get("JOIN_DELAY_MIN", "30"))
DELAY_MAX = int(os.environ.get("JOIN_DELAY_MAX", "60"))

_stop = asyncio.Event()


def stop() -> None:
    _stop.set()


async def _sleep(seconds: float) -> None:
    try:
        await asyncio.wait_for(_stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def _process_one(group_id: str, link: str) -> bool:
    """Return True if a join attempt was made (success or fail)."""
    log.info("Attempting join: %s", link)
    ok, title, err, flood = await join_group(link)
    now = datetime.now(timezone.utc).isoformat()

    if flood is not None:
        log.warning("FloodWait %ds on %s; waiting.", flood, link)
        await _sleep(flood + 5)
        return False  # not counted, will retry

    state = storage.read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            if ok:
                g["status"] = "joined"
                g["joined_at"] = now
                g["title"] = title
                g["error"] = err  # may carry "already a member"
                storage.bump_joined_today(state)
            else:
                g["status"] = "failed"
                g["error"] = err
            break
    storage.write_state(state)
    log.info("Result %s: ok=%s err=%s", link, ok, err)
    return True


async def run_joiner_loop() -> None:
    log.info("Joiner loop started (limit=%d/day, delay=%d-%ds)", DAILY_LIMIT, DELAY_MIN, DELAY_MAX)
    while not _stop.is_set():
        try:
            state = storage.read_state()
            storage.maybe_reset_daily(state)
            storage.write_state(state)

            if state["stats"]["joined_today"] >= DAILY_LIMIT:
                log.info("Daily limit reached. Sleeping 1h.")
                await _sleep(3600)
                continue

            pending = [g for g in state["groups"] if g["status"] == "pending"]
            if not pending:
                await _sleep(30)
                continue

            target = pending[0]
            attempted = await _process_one(target["id"], target["link"])
            if attempted:
                delay = random.randint(DELAY_MIN, DELAY_MAX)
                log.info("Sleeping %ds before next join.", delay)
                await _sleep(delay)
        except Exception as e:
            log.exception("Loop error: %s", e)
            await _sleep(60)
    log.info("Joiner loop stopped.")
