"""주기 입장 스케줄러 — 대용량 리스트 안전 모드."""
from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from . import storage
from .telegram_client import join_group

log = logging.getLogger("scheduler")

# 기본값: 최대 속도 모드 (FloodWait가 실제 리미트 역할)
# 각 리미트 0 = 무제한/비활성
DAILY_JOIN_LIMIT = int(os.environ.get("DAILY_JOIN_LIMIT", "0"))     # 0 = 무제한
HOURLY_JOIN_LIMIT = int(os.environ.get("HOURLY_JOIN_LIMIT", "0"))   # 0 = 무제한
JOIN_DELAY_MIN = int(os.environ.get("JOIN_DELAY_MIN", "60"))
JOIN_DELAY_MAX = int(os.environ.get("JOIN_DELAY_MAX", "180"))
LONG_BREAK_EVERY = int(os.environ.get("LONG_BREAK_EVERY", "0"))     # 0 = 휴식 없음
LONG_BREAK_MIN = int(os.environ.get("LONG_BREAK_MIN", "1800"))
LONG_BREAK_MAX = int(os.environ.get("LONG_BREAK_MAX", "3600"))
QUIET_HOURS_KST = os.environ.get("QUIET_HOURS_KST", "")             # "" = 비활성

KST = timezone(timedelta(hours=9))

_stop = asyncio.Event()


def stop() -> None:
    _stop.set()


async def _sleep(seconds: float) -> None:
    try:
        await asyncio.wait_for(_stop.wait(), timeout=max(1, seconds))
    except asyncio.TimeoutError:
        pass


def _parse_quiet_hours() -> tuple[int, int, int, int] | None:
    """'HH:MM-HH:MM' -> (sh, sm, eh, em). 빈 문자열이면 None."""
    if not QUIET_HOURS_KST or QUIET_HOURS_KST.lower() in ("off", "none", "disabled"):
        return None
    try:
        a, b = QUIET_HOURS_KST.split("-")
        sh, sm = map(int, a.split(":"))
        eh, em = map(int, b.split(":"))
        return sh, sm, eh, em
    except Exception:
        return None


def _in_quiet_hours(now_kst: datetime) -> bool:
    parsed = _parse_quiet_hours()
    if not parsed:
        return False
    sh, sm, eh, em = parsed
    start_min = sh * 60 + sm
    end_min = eh * 60 + em
    cur_min = now_kst.hour * 60 + now_kst.minute
    if start_min == end_min:
        return False
    if start_min < end_min:
        return start_min <= cur_min < end_min
    # 자정 교차
    return cur_min >= start_min or cur_min < end_min


def _seconds_to_quiet_end(now_kst: datetime) -> int:
    parsed = _parse_quiet_hours()
    if not parsed:
        return 60
    sh, sm, eh, em = parsed
    end = now_kst.replace(hour=eh, minute=em, second=0, microsecond=0)
    if end <= now_kst:
        end += timedelta(days=1)
    return int((end - now_kst).total_seconds()) + 5


def _effective_daily_limit(state: dict) -> int:
    """FloodWait 누적에 따른 자동 스로틀.
    반환값: -1 = 무제한, 0 = 일시정지, N = 해당 숫자 한도.
    """
    fw_count = storage.prune_floodwait(state)
    if fw_count >= 4:
        return 0  # 24시간 일시정지
    # 무제한 모드에서 FloodWait 누적되면 점진적으로 좁혀짐
    if DAILY_JOIN_LIMIT == 0:
        if fw_count == 3:
            return 20
        if fw_count == 2:
            return 50
        return -1  # 무제한
    if fw_count == 3:
        return max(1, DAILY_JOIN_LIMIT // 4)
    if fw_count == 2:
        return max(1, DAILY_JOIN_LIMIT // 2)
    return DAILY_JOIN_LIMIT


async def _process_one(group_id: str, link: str) -> tuple[bool, bool]:
    """Return (attempted, is_floodwait)."""
    log.info("Attempting join: %s", link)
    ok, title, err, flood = await join_group(link)

    if flood is not None:
        log.warning("FloodWait %ds on %s; waiting (+30s buffer).", flood, link)
        state = storage.read_state()
        storage.record_floodwait(state)
        storage.write_state(state)
        await _sleep(flood + 30)
        return False, True

    now = datetime.now(timezone.utc).isoformat()
    state = storage.read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            if ok:
                g["status"] = "joined"
                g["joined_at"] = now
                g["title"] = title
                g["error"] = err  # "already a member" 같은 메시지 유지
                storage.bump_join_counters(state)
            else:
                g["status"] = "failed"
                g["error"] = err
            break
    storage.write_state(state)
    log.info("Result %s: ok=%s err=%s", link, ok, err)
    return True, False


async def run_joiner_loop() -> None:
    log.info(
        "Joiner loop started "
        "(daily=%d, hourly=%d, delay=%d~%ds, break every %d joins, quiet=%s KST)",
        DAILY_JOIN_LIMIT, HOURLY_JOIN_LIMIT, JOIN_DELAY_MIN, JOIN_DELAY_MAX,
        LONG_BREAK_EVERY, QUIET_HOURS_KST,
    )
    while not _stop.is_set():
        try:
            state = storage.read_state()
            storage.maybe_reset_counters(state)

            # pause_until 체크
            pu = state["stats"].get("pause_until")
            if pu:
                pu_dt = datetime.fromisoformat(pu.replace("Z", "+00:00"))
                if pu_dt > datetime.now(timezone.utc):
                    wait_s = int((pu_dt - datetime.now(timezone.utc)).total_seconds())
                    log.info("Paused until %s (%ds).", pu, wait_s)
                    await _sleep(min(wait_s, 3600))
                    continue
                else:
                    state["stats"]["pause_until"] = None

            # 자동 스로틀된 일일 한도
            eff_limit = _effective_daily_limit(state)
            state["stats"]["effective_daily_limit"] = eff_limit
            storage.write_state(state)

            if eff_limit == 0:
                log.warning("FloodWait 4회+ 감지 → 24시간 일시정지.")
                pause_until = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                state = storage.read_state()
                state["stats"]["pause_until"] = pause_until
                storage.write_state(state)
                continue

            if eff_limit > 0 and state["stats"]["joined_today"] >= eff_limit:
                log.info("Daily limit %d reached. Sleeping until next hour.", eff_limit)
                await _sleep(3600)
                continue

            # Quiet hours (설정돼 있을 때만)
            now_kst = datetime.now(KST)
            if _in_quiet_hours(now_kst):
                wait_s = _seconds_to_quiet_end(now_kst)
                log.info("Quiet hours (%s KST). Sleeping %ds.", QUIET_HOURS_KST, wait_s)
                await _sleep(wait_s)
                continue

            # 시간당 한도 (0이면 스킵)
            if HOURLY_JOIN_LIMIT > 0 and state["stats"]["joined_this_hour"] >= HOURLY_JOIN_LIMIT:
                next_hour = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0
                )
                wait_s = int((next_hour - datetime.now(timezone.utc)).total_seconds()) + 5
                log.info("Hourly limit %d reached. Sleeping %ds.", HOURLY_JOIN_LIMIT, wait_s)
                await _sleep(wait_s)
                continue

            # 긴 휴식 (0이면 스킵)
            if LONG_BREAK_EVERY > 0 and state["stats"].get("joins_since_break", 0) >= LONG_BREAK_EVERY:
                rest = random.randint(LONG_BREAK_MIN, LONG_BREAK_MAX)
                log.info("Long break (%d joins done). Sleeping %ds.", LONG_BREAK_EVERY, rest)
                state["stats"]["joins_since_break"] = 0
                storage.write_state(state)
                await _sleep(rest)
                continue

            pending = [g for g in state["groups"] if g["status"] == "pending"]
            if not pending:
                await _sleep(60)
                continue

            target = pending[0]
            attempted, was_flood = await _process_one(target["id"], target["link"])
            if attempted:
                delay = random.randint(JOIN_DELAY_MIN, JOIN_DELAY_MAX)
                log.info("Sleeping %ds before next join.", delay)
                await _sleep(delay)
            elif was_flood:
                # 이미 join_group 안에서 flood seconds 대기했음; 짧게 추가
                await _sleep(random.randint(60, 180))
        except Exception as e:
            log.exception("Loop error: %s", e)
            await _sleep(60)
    log.info("Joiner loop stopped.")
