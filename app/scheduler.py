"""다중 계정 워커 풀 스케줄러."""
from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from . import storage
from .telegram_client import join_group

log = logging.getLogger("scheduler")

DAILY_JOIN_LIMIT = int(os.environ.get("DAILY_JOIN_LIMIT", "0"))
HOURLY_JOIN_LIMIT = int(os.environ.get("HOURLY_JOIN_LIMIT", "0"))
JOIN_DELAY_MIN = int(os.environ.get("JOIN_DELAY_MIN", "90"))
JOIN_DELAY_MAX = int(os.environ.get("JOIN_DELAY_MAX", "180"))
LONG_BREAK_EVERY = int(os.environ.get("LONG_BREAK_EVERY", "0"))
LONG_BREAK_MIN = int(os.environ.get("LONG_BREAK_MIN", "1800"))
LONG_BREAK_MAX = int(os.environ.get("LONG_BREAK_MAX", "3600"))
QUIET_HOURS_KST = os.environ.get("QUIET_HOURS_KST", "")
# 다중 계정용: 시작 시각 랜덤 지연 (burst 방지)
WORKER_START_JITTER_MAX = int(os.environ.get("WORKER_START_JITTER_MAX", "600"))
# 전역 Kill switch: 모든 계정 합쳐 24h FW 이벤트 X회 이상 시 전체 일시정지
GLOBAL_FW_PAUSE_THRESHOLD = int(os.environ.get("GLOBAL_FW_PAUSE_THRESHOLD", "10"))

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


def _effective_daily_limit(state: dict, acc_name: str) -> int:
    """계정별 FloodWait 누적에 따른 자동 스로틀. -1=무제한, 0=정지."""
    fw_count = storage.prune_floodwait(state, acc_name)
    if fw_count >= 4:
        return 0
    if DAILY_JOIN_LIMIT == 0:
        if fw_count == 3:
            return 20
        if fw_count == 2:
            return 50
        return -1
    if fw_count == 3:
        return max(1, DAILY_JOIN_LIMIT // 4)
    if fw_count == 2:
        return max(1, DAILY_JOIN_LIMIT // 2)
    return DAILY_JOIN_LIMIT


async def run_worker(account_name: str) -> None:
    """단일 계정 워커 루프."""
    # 시작 시각 랜덤 지연
    jitter = random.randint(0, WORKER_START_JITTER_MAX)
    log.info("[%s] Worker starting in %ds (jitter).", account_name, jitter)
    await _sleep(jitter)
    if _stop.is_set():
        return
    log.info("[%s] Worker active.", account_name)

    while not _stop.is_set():
        try:
            state = storage.read_state()
            storage.maybe_reset_counters(state, account_name)
            storage.write_state(state)

            # 전역 Kill switch
            global_fw = storage.prune_global_floodwait(state)
            if global_fw >= GLOBAL_FW_PAUSE_THRESHOLD:
                log.warning(
                    "[%s] Global FW threshold (%d) reached. Pausing 1h.",
                    account_name, GLOBAL_FW_PAUSE_THRESHOLD,
                )
                await _sleep(3600)
                continue

            # 계정별 pause_until 체크
            acc = state["stats"]["accounts"][account_name]
            pu = acc.get("pause_until")
            if pu:
                pu_dt = datetime.fromisoformat(pu.replace("Z", "+00:00"))
                if pu_dt > datetime.now(timezone.utc):
                    wait_s = int((pu_dt - datetime.now(timezone.utc)).total_seconds())
                    log.info("[%s] Paused until %s (%ds).", account_name, pu, wait_s)
                    await _sleep(min(wait_s, 3600))
                    continue
                else:
                    acc["pause_until"] = None
                    storage.write_state(state)

            # 자동 스로틀
            eff_limit = _effective_daily_limit(state, account_name)
            acc["effective_daily_limit"] = eff_limit
            storage.write_state(state)

            if eff_limit == 0:
                log.warning(
                    "[%s] FloodWait 4회+ 감지 → 24시간 일시정지.", account_name
                )
                pause_until = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                state = storage.read_state()
                state["stats"]["accounts"][account_name]["pause_until"] = pause_until
                storage.write_state(state)
                continue

            if eff_limit > 0 and acc["joined_today"] >= eff_limit:
                log.info("[%s] Daily limit %d reached. Sleeping 1h.", account_name, eff_limit)
                await _sleep(3600)
                continue

            # Quiet hours
            now_kst = datetime.now(KST)
            if _in_quiet_hours(now_kst):
                wait_s = _seconds_to_quiet_end(now_kst)
                log.info("[%s] Quiet hours. Sleeping %ds.", account_name, wait_s)
                await _sleep(wait_s)
                continue

            # 시간당 한도
            if HOURLY_JOIN_LIMIT > 0 and acc["joined_this_hour"] >= HOURLY_JOIN_LIMIT:
                next_hour = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0
                )
                wait_s = int((next_hour - datetime.now(timezone.utc)).total_seconds()) + 5
                log.info("[%s] Hourly limit %d reached. Sleeping %ds.", account_name, HOURLY_JOIN_LIMIT, wait_s)
                await _sleep(wait_s)
                continue

            # 긴 휴식
            if LONG_BREAK_EVERY > 0 and acc.get("joins_since_break", 0) >= LONG_BREAK_EVERY:
                rest = random.randint(LONG_BREAK_MIN, LONG_BREAK_MAX)
                log.info("[%s] Long break. Sleeping %ds.", account_name, rest)
                state = storage.read_state()
                state["stats"]["accounts"][account_name]["joins_since_break"] = 0
                storage.write_state(state)
                await _sleep(rest)
                continue

            # pending 하나 claim
            target = storage.claim_next_pending(account_name)
            if target is None:
                await _sleep(60)
                continue

            # 실제 가입 시도
            link = target["link"]
            log.info("[%s] Attempting join: %s", account_name, link)
            ok, title, err, flood = await join_group(account_name, link)

            if flood is not None:
                log.warning("[%s] FloodWait %ds on %s; waiting (+30s).", account_name, flood, link)
                state = storage.read_state()
                storage.record_floodwait(state, account_name)
                storage.write_state(state)
                storage.return_to_pending(target["id"])
                await _sleep(flood + 30)
                continue

            if ok:
                storage.finalize_group(target["id"], ok, title, err, account_name)
                state = storage.read_state()
                storage.bump_join_counters(state, account_name)
                storage.write_state(state)
                log.info("[%s] Result %s: ok=%s err=%s", account_name, link, ok, err)
            else:
                # 입장 실패 → 그룹 자동 삭제 (ChannelsTooMuch 같은 계정 이슈 제외)
                err_lower = (err or "").lower()
                keep_as_failed = "too many channels" in err_lower
                if keep_as_failed:
                    storage.finalize_group(target["id"], ok, title, err, account_name)
                    log.warning("[%s] Join FAIL (kept): %s err=%s", account_name, link, err)
                else:
                    storage.delete_group(target["id"])
                    log.warning("[%s] Join FAIL & DELETED: %s err=%s", account_name, link, err)

            # 다음 시도까지 대기
            delay = random.randint(JOIN_DELAY_MIN, JOIN_DELAY_MAX)
            log.info("[%s] Sleeping %ds before next join.", account_name, delay)
            await _sleep(delay)
        except Exception as e:
            log.exception("[%s] Loop error: %s", account_name, e)
            await _sleep(60)
    log.info("[%s] Worker stopped.", account_name)
