"""groups.json 원자적 읽기/쓰기 + 시드 로딩 + 다중 계정 지원."""
from __future__ import annotations

import json
import os
import random
import re
import threading
import uuid
from datetime import datetime, timezone
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
    }


def _empty_state() -> dict[str, Any]:
    return {
        "groups": [],
        "stats": {
            "accounts": {},  # name -> account_stats
            "global_floodwait_events": [],
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
    # groups 항목에 claimed_by 필드 추가
    for g in state.get("groups", []):
        g.setdefault("claimed_by", None)
        g.setdefault("worker", None)
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
