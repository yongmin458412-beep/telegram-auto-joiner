"""groups.json 원자적 읽기/쓰기 + 시드 로딩 + 다중 계정 지원."""
from __future__ import annotations

import json
import os
import random
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DATA_FILE = DATA_DIR / "groups.json"
SEED_FILE = Path(os.environ.get("SEED_FILE", "seed_links.txt"))

_lock = threading.Lock()

_LINK_RE = re.compile(r"https?://t\.me/[^\s]+", re.IGNORECASE)


def _empty_account_stats() -> dict[str, Any]:
    return {
        "joined_today": 0,
        "last_reset": datetime.now(timezone.utc).date().isoformat(),
        "joined_this_hour": 0,
        "hour_bucket": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H"),
        "joins_since_break": 0,
        "floodwait_events": [],
        "effective_daily_limit": None,
        "pause_until": None,
        "last_activity": None,
        # forward 전용
        "forwarded_today": 0,
        "forward_last_reset": datetime.now(timezone.utc).date().isoformat(),
        "forwarded_total": 0,
        "forward_floodwait_events": [],
        "forward_effective_limit": None,
        "forward_pause_until": None,
        "forward_last_activity": None,
        # round 기반 전달
        "forward_rounds_today": 0,
        "forward_rounds_last_reset": datetime.now(timezone.utc).date().isoformat(),
        "forward_current_round_remaining": [],
        "forward_current_round_total": 0,
        "forward_next_round_at": None,
    }


def _empty_forward_account() -> dict[str, Any]:
    return {
        "source_link": "",
        "source_message_id": 0,
        "source_chat_id": None,
        "source_accessible": False,
        "last_check": None,
        "last_check_error": None,
    }


def _empty_state() -> dict[str, Any]:
    return {
        "groups": [],
        "stats": {
            "accounts": {},  # name -> account_stats
            "global_floodwait_events": [],
            "global_forward_floodwait_events": [],
        },
        "forward_config": {
            "enabled": False,
            "initial_delay_hours": 24,
            "accounts": {},
            "updated_at": None,
        },
    }


def _ensure_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        _atomic_write(_empty_state())


def _atomic_write(state: dict[str, Any]) -> None:
    tmp = DATA_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DATA_FILE)


def _migrate(state: dict[str, Any]) -> dict[str, Any]:
    """기존 스키마 마이그레이션."""
    stats = state.setdefault("stats", {})
    # 싱글 계정 -> 다중 계정 구조로
    if "accounts" not in stats:
        old = {
            "joined_today": stats.get("joined_today", 0),
            "last_reset": stats.get("last_reset", datetime.now(timezone.utc).date().isoformat()),
            "joined_this_hour": stats.get("joined_this_hour", 0),
            "hour_bucket": stats.get("hour_bucket", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")),
            "joins_since_break": stats.get("joins_since_break", 0),
            "floodwait_events": stats.get("floodwait_events", []),
            "effective_daily_limit": stats.get("effective_daily_limit"),
            "pause_until": stats.get("pause_until"),
            "last_activity": None,
        }
        stats["accounts"] = {"1": old}
    stats.setdefault("global_floodwait_events", [])
    stats.setdefault("global_forward_floodwait_events", [])

    # 계정별 forward 필드 마이그레이션
    today = datetime.now(timezone.utc).date().isoformat()
    for acc in stats["accounts"].values():
        acc.setdefault("forwarded_today", 0)
        acc.setdefault("forward_last_reset", today)
        acc.setdefault("forwarded_total", 0)
        acc.setdefault("forward_floodwait_events", [])
        acc.setdefault("forward_effective_limit", None)
        acc.setdefault("forward_pause_until", None)
        acc.setdefault("forward_last_activity", None)
        # round 필드
        acc.setdefault("forward_rounds_today", 0)
        acc.setdefault("forward_rounds_last_reset", today)
        acc.setdefault("forward_current_round_remaining", [])
        acc.setdefault("forward_current_round_total", 0)
        acc.setdefault("forward_next_round_at", None)

    # 그룹 필드 마이그레이션
    for g in state.get("groups", []):
        g.setdefault("claimed_by", None)
        g.setdefault("worker", None)
        g.setdefault("forward_enabled", False)
        g.setdefault("forward_count", 0)
        g.setdefault("last_forwarded_at", None)
        g.setdefault("last_forward_error", None)

    # forward_config 마이그레이션
    fc = state.setdefault("forward_config", {})
    fc.setdefault("enabled", False)
    fc.setdefault("initial_delay_hours", 24)
    fc.setdefault("accounts", {})
    fc.setdefault("updated_at", None)

    return state


def read_state() -> dict[str, Any]:
    with _lock:
        _ensure_file()
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        return _migrate(state)


def write_state(state: dict[str, Any]) -> None:
    with _lock:
        _ensure_file()
        _atomic_write(state)


def ensure_account(state: dict[str, Any], name: str) -> dict[str, Any]:
    acc = state["stats"]["accounts"].get(name)
    if acc is None:
        acc = _empty_account_stats()
        state["stats"]["accounts"][name] = acc
    return acc


# ---------- 링크 정규화 ----------

def normalize_link(link: str) -> Optional[str]:
    if not link:
        return None
    s = link.strip()
    if not s:
        return None

    m = re.fullmatch(r"@([A-Za-z][A-Za-z0-9_]{3,})", s)
    if m:
        return f"https://t.me/{m.group(1).lower()}"

    m = re.fullmatch(r"https?://t\.me/(.+?)/?", s, re.IGNORECASE)
    if m:
        rest = m.group(1).strip()
        if rest.startswith("+"):
            return f"https://t.me/{rest}"
        if rest.lower().startswith("joinchat/"):
            return f"https://t.me/{rest}"
        return f"https://t.me/{rest.lower()}"

    m = re.fullmatch(r"([A-Za-z][A-Za-z0-9_]{3,})", s)
    if m:
        return f"https://t.me/{m.group(1).lower()}"

    return None


def _dedup_key(normalized: str) -> str:
    return normalized


# ---------- 그룹 CRUD ----------

def _existing_keys(state: dict[str, Any]) -> set[str]:
    keys = set()
    for g in state["groups"]:
        n = normalize_link(g["link"])
        if n:
            keys.add(_dedup_key(n))
    return keys


def add_group(link: str) -> Optional[dict[str, Any]]:
    normalized = normalize_link(link)
    if not normalized:
        return None
    state = read_state()
    if _dedup_key(normalized) in _existing_keys(state):
        return None
    now = datetime.now(timezone.utc).isoformat()
    entry = {
        "id": uuid.uuid4().hex[:12],
        "link": normalized,
        "status": "pending",
        "added_at": now,
        "joined_at": None,
        "error": None,
        "title": None,
        "claimed_by": None,
        "worker": None,
    }
    state["groups"].append(entry)
    write_state(state)
    return entry


def add_groups_bulk(links: list[str]) -> tuple[int, int]:
    state = read_state()
    existing = _existing_keys(state)
    added = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()
    for raw in links:
        normalized = normalize_link(raw)
        if not normalized:
            skipped += 1
            continue
        key = _dedup_key(normalized)
        if key in existing:
            skipped += 1
            continue
        existing.add(key)
        state["groups"].append({
            "id": uuid.uuid4().hex[:12],
            "link": normalized,
            "status": "pending",
            "added_at": now,
            "joined_at": None,
            "error": None,
            "title": None,
            "claimed_by": None,
            "worker": None,
        })
        added += 1
    if added:
        write_state(state)
    return added, skipped


def delete_group(group_id: str) -> bool:
    state = read_state()
    before = len(state["groups"])
    state["groups"] = [g for g in state["groups"] if g["id"] != group_id]
    changed = len(state["groups"]) != before
    if changed:
        write_state(state)
    return changed


def reset_to_pending(group_id: str) -> None:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["status"] = "pending"
            g["error"] = None
            g["joined_at"] = None
            g["claimed_by"] = None
            break
    write_state(state)


def release_stale_claims() -> int:
    """앱 시작 시 processing 상태 → pending 으로 되돌림."""
    state = read_state()
    released = 0
    for g in state["groups"]:
        if g["status"] == "processing":
            g["status"] = "pending"
            g["claimed_by"] = None
            released += 1
    if released:
        write_state(state)
    return released


def claim_next_pending(worker_name: str) -> Optional[dict[str, Any]]:
    """원자적으로 pending 하나 집어서 processing 으로 변경."""
    with _lock:
        _ensure_file()
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        state = _migrate(state)
        target = None
        for g in state["groups"]:
            if g["status"] == "pending":
                target = g
                break
        if target is None:
            return None
        target["status"] = "processing"
        target["claimed_by"] = worker_name
        _atomic_write(state)
        return dict(target)


def finalize_group(
    group_id: str,
    ok: bool,
    title: Optional[str],
    error: Optional[str],
    worker_name: str,
) -> None:
    state = read_state()
    now = datetime.now(timezone.utc).isoformat()
    for g in state["groups"]:
        if g["id"] == group_id:
            if ok:
                g["status"] = "joined"
                g["joined_at"] = now
                g["title"] = title
                g["error"] = error
            else:
                g["status"] = "failed"
                g["error"] = error
            g["worker"] = worker_name
            g["claimed_by"] = None
            break
    write_state(state)


def return_to_pending(group_id: str) -> None:
    """FloodWait 등으로 처리 실패 시 pending 으로 되돌림."""
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["status"] = "pending"
            g["claimed_by"] = None
            break
    write_state(state)


# ---------- 시드 로딩 ----------

def load_seed_links() -> list[str]:
    if not SEED_FILE.exists():
        return []
    text = SEED_FILE.read_text(encoding="utf-8", errors="ignore")
    found = _LINK_RE.findall(text)
    unique: dict[str, str] = {}
    for raw in found:
        n = normalize_link(raw)
        if n:
            unique.setdefault(_dedup_key(n), n)
    links = list(unique.values())
    random.shuffle(links)
    return links


def import_seed_merge() -> tuple[int, int]:
    links = load_seed_links()
    return add_groups_bulk(links)


# ---------- 계정별 통계 업데이트 ----------

def maybe_reset_counters(state: dict[str, Any], acc_name: str) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    acc = ensure_account(state, acc_name)
    if acc.get("last_reset") != today:
        acc["joined_today"] = 0
        acc["last_reset"] = today
    if acc.get("hour_bucket") != hour:
        acc["joined_this_hour"] = 0
        acc["hour_bucket"] = hour


def bump_join_counters(state: dict[str, Any], acc_name: str) -> None:
    maybe_reset_counters(state, acc_name)
    acc = ensure_account(state, acc_name)
    acc["joined_today"] += 1
    acc["joined_this_hour"] += 1
    acc["joins_since_break"] = acc.get("joins_since_break", 0) + 1
    acc["last_activity"] = datetime.now(timezone.utc).isoformat()


def record_floodwait(state: dict[str, Any], acc_name: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    acc = ensure_account(state, acc_name)
    events = acc.get("floodwait_events", [])
    events.append(now_iso)
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    acc["floodwait_events"] = events
    # 전역 기록도 갱신
    gevents = state["stats"].setdefault("global_floodwait_events", [])
    gevents.append(now_iso)
    gevents[:] = [
        e for e in gevents
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]


def prune_floodwait(state: dict[str, Any], acc_name: str) -> int:
    acc = ensure_account(state, acc_name)
    events = acc.get("floodwait_events", [])
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    acc["floodwait_events"] = events
    return len(events)


def prune_global_floodwait(state: dict[str, Any]) -> int:
    events = state["stats"].get("global_floodwait_events", [])
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    state["stats"]["global_floodwait_events"] = events
    return len(events)


# ---------- Forward 기능 ----------

def set_forward_config_account(acc_name: str, source_link: str, message_id: int) -> None:
    state = read_state()
    normalized = normalize_link(source_link) or source_link.strip()
    fc = state.setdefault("forward_config", {})
    accounts = fc.setdefault("accounts", {})
    prev = accounts.get(acc_name) or _empty_forward_account()
    # 링크 또는 메시지 ID가 바뀌면 resolved 캐시 무효화
    changed = prev.get("source_link") != normalized or prev.get("source_message_id") != int(message_id)
    accounts[acc_name] = {
        **_empty_forward_account(),
        "source_link": normalized,
        "source_message_id": int(message_id),
        "source_chat_id": None if changed else prev.get("source_chat_id"),
        "source_accessible": False if changed else prev.get("source_accessible", False),
        "last_check": None if changed else prev.get("last_check"),
        "last_check_error": None if changed else prev.get("last_check_error"),
    }
    fc["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_state(state)


def set_forward_enabled_global(enabled: bool) -> None:
    state = read_state()
    state.setdefault("forward_config", {})["enabled"] = bool(enabled)
    state["forward_config"]["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_state(state)


def set_forward_initial_delay(hours: float) -> None:
    state = read_state()
    state.setdefault("forward_config", {})["initial_delay_hours"] = float(hours)
    state["forward_config"]["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_state(state)


def set_source_resolved(
    acc_name: str,
    chat_id: Optional[int],
    accessible: bool,
    error: Optional[str] = None,
) -> None:
    state = read_state()
    fc = state.setdefault("forward_config", {})
    accounts = fc.setdefault("accounts", {})
    acc = accounts.get(acc_name) or _empty_forward_account()
    acc["source_chat_id"] = chat_id
    acc["source_accessible"] = bool(accessible)
    acc["last_check"] = datetime.now(timezone.utc).isoformat()
    acc["last_check_error"] = error
    accounts[acc_name] = acc
    write_state(state)


def toggle_forward(group_id: str, enabled: bool) -> None:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["forward_enabled"] = bool(enabled)
            break
    write_state(state)


def set_forward_enabled_bulk(group_ids: list[str], enabled: bool) -> int:
    state = read_state()
    ids = set(group_ids)
    count = 0
    for g in state["groups"]:
        if g["id"] in ids:
            g["forward_enabled"] = bool(enabled)
            count += 1
    if count:
        write_state(state)
    return count


def enable_forward_all_joined() -> int:
    """지금까지 joined 된 그룹 전부 forward_enabled=True."""
    state = read_state()
    count = 0
    for g in state["groups"]:
        if g["status"] == "joined" and not g.get("forward_enabled"):
            g["forward_enabled"] = True
            count += 1
    if count:
        write_state(state)
    return count


def disable_forward_all() -> int:
    state = read_state()
    count = 0
    for g in state["groups"]:
        if g.get("forward_enabled"):
            g["forward_enabled"] = False
            count += 1
    if count:
        write_state(state)
    return count


def reset_forward_count(group_id: str) -> None:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["forward_count"] = 0
            g["last_forwarded_at"] = None
            break
    write_state(state)


def get_forward_targets(acc_name: str, initial_delay_hours: float) -> list[dict[str, Any]]:
    """해당 계정이 전달할 수 있는 그룹 목록 (우선순위 정렬)."""
    state = read_state()
    cutoff_ts = datetime.now(timezone.utc).timestamp() - initial_delay_hours * 3600
    targets: list[dict[str, Any]] = []
    for g in state["groups"]:
        if g.get("status") != "joined":
            continue
        if g.get("worker") != acc_name:
            continue
        if not g.get("forward_enabled"):
            continue
        joined_at = g.get("joined_at")
        if not joined_at:
            continue
        try:
            joined_ts = datetime.fromisoformat(joined_at.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if joined_ts > cutoff_ts:
            continue  # 초기 지연 미충족
        targets.append(g)

    def sort_key(g):
        # last_forwarded_at NULLS FIRST (한 번도 안 보낸 그룹 먼저)
        last = g.get("last_forwarded_at")
        return (0, "") if not last else (1, last)

    targets.sort(key=sort_key)
    return [dict(g) for g in targets]


def record_forward_success(group_id: str, acc_name: str) -> int:
    """성공 기록. 반환값: 업데이트된 forward_count (N)."""
    state = read_state()
    new_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["forward_count"] = int(g.get("forward_count", 0)) + 1
            g["last_forwarded_at"] = now_iso
            g["last_forward_error"] = None
            new_count = g["forward_count"]
            break
    # 계정 통계 업데이트
    maybe_reset_forward_counters(state, acc_name)
    acc = ensure_account(state, acc_name)
    acc["forwarded_today"] = acc.get("forwarded_today", 0) + 1
    acc["forwarded_total"] = acc.get("forwarded_total", 0) + 1
    acc["forward_last_activity"] = now_iso
    write_state(state)
    return new_count


def record_forward_error(group_id: str, error: str, disable: bool) -> None:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g["last_forward_error"] = error
            if disable:
                g["forward_enabled"] = False
            break
    write_state(state)


def maybe_reset_forward_counters(state: dict[str, Any], acc_name: str) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    acc = ensure_account(state, acc_name)
    if acc.get("forward_last_reset") != today:
        acc["forwarded_today"] = 0
        acc["forward_last_reset"] = today
    if acc.get("forward_rounds_last_reset") != today:
        acc["forward_rounds_today"] = 0
        acc["forward_rounds_last_reset"] = today


def start_new_round(acc_name: str, target_ids: list[str]) -> None:
    state = read_state()
    acc = ensure_account(state, acc_name)
    acc["forward_current_round_remaining"] = list(target_ids)
    acc["forward_current_round_total"] = len(target_ids)
    acc["forward_next_round_at"] = None
    write_state(state)


def pop_round_target(acc_name: str) -> Optional[str]:
    """현재 라운드에서 다음 처리할 group_id 를 원자적으로 꺼냄."""
    with _lock:
        _ensure_file()
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        state = _migrate(state)
        acc = state["stats"]["accounts"].get(acc_name)
        if not acc:
            return None
        remaining = acc.get("forward_current_round_remaining", [])
        if not remaining:
            return None
        gid = remaining.pop(0)
        acc["forward_current_round_remaining"] = remaining
        _atomic_write(state)
        return gid


def complete_round(acc_name: str, interval_hours: float) -> int:
    """라운드 1개 완료 처리. 다음 라운드 시각 설정."""
    state = read_state()
    acc = ensure_account(state, acc_name)
    acc["forward_rounds_today"] = int(acc.get("forward_rounds_today", 0)) + 1
    next_at = (
        datetime.now(timezone.utc) + timedelta(hours=interval_hours)
    ).isoformat()
    acc["forward_next_round_at"] = next_at
    acc["forward_current_round_remaining"] = []
    acc["forward_current_round_total"] = 0
    write_state(state)
    return acc["forward_rounds_today"]


def get_group_by_id(group_id: str) -> Optional[dict[str, Any]]:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            return dict(g)
    return None


def record_forward_floodwait(state: dict[str, Any], acc_name: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    acc = ensure_account(state, acc_name)
    events = acc.get("forward_floodwait_events", [])
    events.append(now_iso)
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    acc["forward_floodwait_events"] = events
    gevents = state["stats"].setdefault("global_forward_floodwait_events", [])
    gevents.append(now_iso)
    gevents[:] = [
        e for e in gevents
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]


def prune_forward_floodwait(state: dict[str, Any], acc_name: str) -> int:
    acc = ensure_account(state, acc_name)
    events = acc.get("forward_floodwait_events", [])
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    acc["forward_floodwait_events"] = events
    return len(events)


def prune_global_forward_fw(state: dict[str, Any]) -> int:
    events = state["stats"].get("global_forward_floodwait_events", [])
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    state["stats"]["global_forward_floodwait_events"] = events
    return len(events)
