"""메시지 라운드 기반 전달 워커 풀.

1 라운드 = forward_enabled 그룹 전체에 순차 전달 (계정별).
하루에 DAILY_ROUND_LIMIT 만큼 라운드 반복.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from . import storage
from .telegram_client import forward_and_counter, resolve_source_channel

log = logging.getLogger("forwarder")

# 라운드 기반 설정 (지속 가능한 최대치)
DAILY_ROUND_LIMIT = int(os.environ.get("DAILY_ROUND_LIMIT", "3"))
ROUND_INTERVAL_HOURS = float(os.environ.get("ROUND_INTERVAL_HOURS", "6"))

# 라운드 내 그룹 간 딜레이 (burst 회피)
WITHIN_ROUND_DELAY_MIN = int(os.environ.get("WITHIN_ROUND_DELAY_MIN", "90"))
WITHIN_ROUND_DELAY_MAX = int(os.environ.get("WITHIN_ROUND_DELAY_MAX", "180"))

# 절대 안전 한도 (0 = 없음)
DAILY_FORWARD_LIMIT = int(os.environ.get("DAILY_FORWARD_LIMIT", "0"))

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


def _effective_round_limit(state: dict, acc_name: str) -> int:
    """FloodWait 누적에 따른 라운드 한도 자동 스로틀."""
    fw = storage.prune_forward_floodwait(state, acc_name)
    base = DAILY_ROUND_LIMIT
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


async def _forward_one_group(
    group_id: str, account_name: str, source_chat_id: int, source_msg_id: int
) -> None:
    """단일 그룹에 대해 forward + counter 메시지 전송 + 결과 기록."""
    g = storage.get_group_by_id(group_id)
    if not g:
        log.info("[%s] group %s no longer exists, skip.", account_name, group_id)
        return
    if not g.get("forward_enabled"):
        log.info("[%s] group %s forward_enabled=False, skip.", account_name, group_id)
        return
    if g.get("worker") != account_name:
        log.info(
            "[%s] group %s owned by worker %s, skip.",
            account_name, group_id, g.get("worker"),
        )
        return

    current_count = int(g.get("forward_count", 0))
    next_count = current_count + 1
    counter_text = f"{next_count}회"

    log.info(
        "[%s] Forwarding to %s (round target, count -> %d)",
        account_name, g["link"], next_count,
    )
    ok, err, flood, disable = await forward_and_counter(
        account_name=account_name,
        target_link=g["link"],
        source_chat_id=source_chat_id,
        source_message_id=source_msg_id,
        counter_text=counter_text,
    )

    if flood is not None:
        state = storage.read_state()
        storage.record_forward_floodwait(state, account_name)
        storage.write_state(state)
        log.warning(
            "[%s] FloodWait %ds on %s. Waiting (+30s).",
            account_name, flood, g["link"],
        )
        await _sleep(flood + 30)
        return

    if ok:
        storage.record_forward_success(group_id, account_name)
        if err:
            storage.record_forward_error(group_id, err, disable=False)
        log.info(
            "[%s] Forward OK: %s (#%d) note=%s",
            account_name, g["link"], next_count, err,
        )
    else:
        if disable:
            storage.delete_group(group_id)
            log.warning(
                "[%s] Forward FAIL & DELETED: %s err=%s",
                account_name, g["link"], err,
            )
        else:
            storage.record_forward_error(group_id, err or "unknown", disable=False)
            log.warning(
                "[%s] Forward FAIL: %s err=%s",
                account_name, g["link"], err,
            )
            if err and (
                "source message id invalid" in err
                or "source channel private" in err
            ):
                storage.set_source_resolved(account_name, None, False, err)


async def run_forward_worker(account_name: str) -> None:
    jitter = random.randint(0, FORWARD_START_JITTER_MAX)
    log.info("[%s] Forward worker starting in %ds.", account_name, jitter)
    await _sleep(jitter)
    if _stop.is_set():
        return
    log.info(
        "[%s] Forward worker active. rounds/day=%d interval=%sh",
        account_name, DAILY_ROUND_LIMIT, ROUND_INTERVAL_HOURS,
    )

    while not _stop.is_set():
        try:
            state = storage.read_state()
            fc = state.get("forward_config", {})

            # 전역 enabled 체크
            if not fc.get("enabled"):
                await _sleep(60)
                continue

            # 계정별 카운터 리셋
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

            # 계정별 pause
            acc = state["stats"]["accounts"].get(account_name, {})
            pu = acc.get("forward_pause_until")
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

            # 라운드 자동 스로틀
            eff_rounds = _effective_round_limit(state, account_name)
            state = storage.read_state()
            state["stats"]["accounts"].setdefault(account_name, {})[
                "forward_effective_limit"
            ] = eff_rounds
            storage.write_state(state)

            if eff_rounds == 0:
                log.warning("[%s] Forward FW 4+. Pausing 24h.", account_name)
                pause_until = (
                    datetime.now(timezone.utc) + timedelta(hours=24)
                ).isoformat()
                state = storage.read_state()
                state["stats"]["accounts"][account_name][
                    "forward_pause_until"
                ] = pause_until
                storage.write_state(state)
                continue

            # 일일 라운드 한도 체크
            acc = state["stats"]["accounts"].get(account_name, {})
            rounds_today = int(acc.get("forward_rounds_today", 0))
            if rounds_today >= eff_rounds:
                log.info(
                    "[%s] Daily rounds %d/%d reached. Sleeping 1h.",
                    account_name, rounds_today, eff_rounds,
                )
                await _sleep(3600)
                continue

            # 절대 안전 한도 (DAILY_FORWARD_LIMIT>0 이면)
            if DAILY_FORWARD_LIMIT > 0 and acc.get("forwarded_today", 0) >= DAILY_FORWARD_LIMIT:
                log.info(
                    "[%s] Absolute forward limit %d reached. Sleeping 1h.",
                    account_name, DAILY_FORWARD_LIMIT,
                )
                await _sleep(3600)
                continue

            # 계정 소스 확인
            acc_cfg = fc.get("accounts", {}).get(account_name)
            if not acc_cfg or not acc_cfg.get("source_accessible"):
                await _sleep(120)
                continue
            source_chat_id = acc_cfg.get("source_chat_id")
            source_msg_id = acc_cfg.get("source_message_id")
            if not source_chat_id or not source_msg_id:
                await _sleep(120)
                continue

            # 현재 라운드 상태 체크
            remaining = acc.get("forward_current_round_remaining", [])
            next_round_at = acc.get("forward_next_round_at")

            if not remaining:
                # 다음 라운드 시각까지 대기 필요?
                if next_round_at:
                    try:
                        nr_dt = datetime.fromisoformat(next_round_at.replace("Z", "+00:00"))
                    except Exception:
                        nr_dt = None
                    if nr_dt and nr_dt > datetime.now(timezone.utc):
                        wait_s = int((nr_dt - datetime.now(timezone.utc)).total_seconds())
                        log.info(
                            "[%s] Waiting %ds until next round (%s).",
                            account_name, wait_s, next_round_at,
                        )
                        await _sleep(min(wait_s, 3600))
                        continue

                # 새 라운드 시작
                initial_delay = float(fc.get("initial_delay_hours", 24))
                targets = storage.get_forward_targets(account_name, initial_delay)
                if not targets:
                    log.info("[%s] No targets for new round. Sleeping 5min.", account_name)
                    await _sleep(300)
                    continue
                target_ids = [t["id"] for t in targets]
                storage.start_new_round(account_name, target_ids)
                log.info(
                    "[%s] ▶ Round %d/%d START with %d groups.",
                    account_name, rounds_today + 1, eff_rounds, len(target_ids),
                )
                # 바로 다음 루프 반복에서 pop 시작

            # 라운드 내 그룹 1개 처리
            gid = storage.pop_round_target(account_name)
            if gid is None:
                # 라운드 종료 직전에 누군가 remaining을 비웠을 수 있음
                continue

            await _forward_one_group(
                gid, account_name, source_chat_id, source_msg_id
            )

            # 라운드가 끝났는지 확인
            state = storage.read_state()
            acc = state["stats"]["accounts"].get(account_name, {})
            if not acc.get("forward_current_round_remaining"):
                completed = storage.complete_round(account_name, ROUND_INTERVAL_HOURS)
                next_at = (
                    datetime.now(timezone.utc)
                    + timedelta(hours=ROUND_INTERVAL_HOURS)
                ).isoformat()
                log.info(
                    "[%s] ✅ Round %d/%d COMPLETE. Next round at %s.",
                    account_name, completed, eff_rounds, next_at,
                )
            else:
                # 라운드 내 다음 그룹까지 짧은 딜레이
                delay = random.randint(WITHIN_ROUND_DELAY_MIN, WITHIN_ROUND_DELAY_MAX)
                log.info("[%s] Within-round sleep %ds.", account_name, delay)
                await _sleep(delay)
        except Exception as e:
            log.exception("[%s] Forward loop error: %s", account_name, e)
            await _sleep(60)
    log.info("[%s] Forward worker stopped.", account_name)
