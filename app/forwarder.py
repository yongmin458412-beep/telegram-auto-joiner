"""메시지 반복 전달 워커 풀 (계정별 1개)."""
from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from . import storage
from .telegram_client import forward_and_counter, resolve_source_channel

log = logging.getLogger("forwarder")

DAILY_FORWARD_LIMIT = int(os.environ.get("DAILY_FORWARD_LIMIT", "10"))
FORWARD_DELAY_MIN = int(os.environ.get("FORWARD_DELAY_MIN", "600"))    # 10분
FORWARD_DELAY_MAX = int(os.environ.get("FORWARD_DELAY_MAX", "2400"))   # 40분
FORWARD_INITIAL_DELAY_HOURS = float(os.environ.get("FORWARD_INITIAL_DELAY_HOURS", "24"))
FORWARD_FW_PAUSE_THRESHOLD = int(os.environ.get("FORWARD_FW_PAUSE_THRESHOLD", "6"))
FORWARD_START_JITTER_MAX = int(os.environ.get("FORWARD_START_JITTER_MAX", "300"))

_stop = asyncio.Event()


def stop() -> None:
    _stop.set()


async def _sleep(seconds: float) -> None:
    try:
        await asyncio.wait_for(_stop.wait(), timeout=max(1, seconds))
    except asyncio.TimeoutError:
        pass


def _effective_daily_limit(state: dict, acc_name: str) -> int:
    """반환: -1=무제한 없음(고정한도), 0=정지, N=해당 한도.
    DAILY_FORWARD_LIMIT 가 0이면 전달 기능 자체가 비활성이지만,
    여기서는 한도가 10처럼 고정값이라 가정.
    """
    fw = storage.prune_forward_floodwait(state, acc_name)
    base = DAILY_FORWARD_LIMIT
    if fw >= 4:
        return 0
    if fw == 3:
        return max(1, base // 5)
    if fw == 2:
        return max(1, base // 2)
    return base


async def resolve_source_all(account_names: list[str]) -> dict[str, dict]:
    """4계정 모두에 대해 자기 소스 채널 resolve."""
    state = storage.read_state()
    fc = state.get("forward_config", {}).get("accounts", {})
    results: dict[str, dict] = {}
    for acc_name in account_names:
        cfg = fc.get(acc_name)
        if not cfg or not cfg.get("source_link") or not cfg.get("source_message_id"):
            results[acc_name] = {"ok": False, "error": "not configured"}
            storage.set_source_resolved(acc_name, None, False, "not configured")
            continue
        try:
            chat_id, accessible, err = await resolve_source_channel(
                acc_name, cfg["source_link"]
            )
            storage.set_source_resolved(acc_name, chat_id, accessible, err)
            results[acc_name] = {
                "ok": accessible,
                "chat_id": chat_id,
                "error": err,
            }
            log.info(
                "[%s] resolve: accessible=%s chat_id=%s err=%s",
                acc_name, accessible, chat_id, err,
            )
        except Exception as e:
            log.exception("[%s] resolve failed: %s", acc_name, e)
            storage.set_source_resolved(acc_name, None, False, str(e))
            results[acc_name] = {"ok": False, "error": str(e)}
    return results


async def run_forward_worker(account_name: str) -> None:
    jitter = random.randint(0, FORWARD_START_JITTER_MAX)
    log.info("[%s] Forward worker starting in %ds.", account_name, jitter)
    await _sleep(jitter)
    if _stop.is_set():
        return
    log.info("[%s] Forward worker active.", account_name)

    while not _stop.is_set():
        try:
            state = storage.read_state()
            fc = state.get("forward_config", {})

            # 전역 enabled 체크
            if not fc.get("enabled"):
                await _sleep(60)
                continue

            # 계정별 forward 카운터 리셋
            storage.maybe_reset_forward_counters(state, account_name)
            storage.write_state(state)

            # 전역 kill switch
            global_fw = storage.prune_global_forward_fw(state)
            if global_fw >= FORWARD_FW_PAUSE_THRESHOLD:
                log.warning(
                    "[%s] Global forward FW threshold (%d) reached. Pausing 1h.",
                    account_name, FORWARD_FW_PAUSE_THRESHOLD,
                )
                await _sleep(3600)
                continue

            # 계정별 pause_until
            acc_stats = state["stats"]["accounts"].get(account_name, {})
            pu = acc_stats.get("forward_pause_until")
            if pu:
                try:
                    pu_dt = datetime.fromisoformat(pu.replace("Z", "+00:00"))
                except Exception:
                    pu_dt = None
                if pu_dt and pu_dt > datetime.now(timezone.utc):
                    wait_s = int((pu_dt - datetime.now(timezone.utc)).total_seconds())
                    log.info("[%s] Forward paused until %s.", account_name, pu)
                    await _sleep(min(wait_s, 3600))
                    continue
                else:
                    state = storage.read_state()
                    state["stats"]["accounts"][account_name]["forward_pause_until"] = None
                    storage.write_state(state)

            # 자동 스로틀된 한도
            eff_limit = _effective_daily_limit(state, account_name)
            state = storage.read_state()
            state["stats"]["accounts"].setdefault(account_name, {})[
                "forward_effective_limit"
            ] = eff_limit
            storage.write_state(state)

            if eff_limit == 0:
                log.warning(
                    "[%s] Forward FW 4+. Pausing 24h.", account_name
                )
                pause_until = (
                    datetime.now(timezone.utc) + timedelta(hours=24)
                ).isoformat()
                state = storage.read_state()
                state["stats"]["accounts"][account_name][
                    "forward_pause_until"
                ] = pause_until
                storage.write_state(state)
                continue

            acc = state["stats"]["accounts"].get(account_name, {})
            forwarded_today = acc.get("forwarded_today", 0)
            if forwarded_today >= eff_limit:
                log.info(
                    "[%s] Forward daily limit %d reached (today=%d). Sleeping 1h.",
                    account_name, eff_limit, forwarded_today,
                )
                await _sleep(3600)
                continue

            # 계정 설정 확인
            acc_cfg = fc.get("accounts", {}).get(account_name)
            if not acc_cfg or not acc_cfg.get("source_accessible"):
                await _sleep(120)
                continue
            source_chat_id = acc_cfg.get("source_chat_id")
            source_msg_id = acc_cfg.get("source_message_id")
            if not source_chat_id or not source_msg_id:
                await _sleep(120)
                continue

            # 다음 대상 선정
            initial_delay = float(fc.get("initial_delay_hours", 24))
            targets = storage.get_forward_targets(account_name, initial_delay)
            if not targets:
                await _sleep(300)
                continue

            target = targets[0]
            current_count = int(target.get("forward_count", 0))
            next_count = current_count + 1
            counter_text = f"{next_count}회"

            log.info(
                "[%s] Forwarding to %s (count -> %d)",
                account_name, target["link"], next_count,
            )
            ok, err, flood, disable = await forward_and_counter(
                account_name=account_name,
                target_link=target["link"],
                source_chat_id=source_chat_id,
                source_message_id=source_msg_id,
                counter_text=counter_text,
            )

            # FloodWait 기록
            if flood is not None:
                state = storage.read_state()
                storage.record_forward_floodwait(state, account_name)
                storage.write_state(state)
                await _sleep(flood + 30)
                continue

            if ok:
                storage.record_forward_success(target["id"], account_name)
                if err:
                    storage.record_forward_error(target["id"], err, disable=False)
                log.info(
                    "[%s] Forward OK: %s (#%d) note=%s",
                    account_name, target["link"], next_count, err,
                )
            else:
                # 영구적 실패(쓰기 금지/차단) → 그룹 자동 삭제
                if disable:
                    storage.delete_group(target["id"])
                    log.warning(
                        "[%s] Forward FAIL & DELETED: %s err=%s",
                        account_name, target["link"], err,
                    )
                else:
                    storage.record_forward_error(target["id"], err or "unknown", disable=False)
                    log.warning(
                        "[%s] Forward FAIL: %s err=%s",
                        account_name, target["link"], err,
                    )
                    # 소스 관련 에러 힌트
                    if err and (
                        "source message id invalid" in err
                        or "source channel private" in err
                    ):
                        storage.set_source_resolved(
                            account_name, None, False, err
                        )

            # 다음까지 대기
            delay = random.randint(FORWARD_DELAY_MIN, FORWARD_DELAY_MAX)
            log.info("[%s] Forward sleeping %ds.", account_name, delay)
            await _sleep(delay)
        except Exception as e:
            log.exception("[%s] Forward loop error: %s", account_name, e)
            await _sleep(60)
    log.info("[%s] Forward worker stopped.", account_name)
