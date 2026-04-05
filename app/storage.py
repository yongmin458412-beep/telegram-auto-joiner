"""groups.json 원자적 읽기/쓰기 + 시드 로딩."""
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
SEED_FILE = Path("data/seed_links.txt")  # repo 내 번들

_lock = threading.Lock()

_LINK_RE = re.compile(r"https?://t\.me/[^\s]+", re.IGNORECASE)


def _empty_state() -> dict[str, Any]:
    today = datetime.now(timezone.utc).date().isoformat()
    return {
        "groups": [],
        "stats": {
            "joined_today": 0,
            "last_reset": today,
            "joined_this_hour": 0,
            "hour_bucket": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H"),
            "joins_since_break": 0,
            "floodwait_events": [],  # ISO timestamps, rolling 24h
            "effective_daily_limit": None,  # None = use env default
            "pause_until": None,  # ISO timestamp
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
    """기존 스키마에 신규 stats 필드 채워넣기."""
    defaults = _empty_state()["stats"]
    for k, v in defaults.items():
        state["stats"].setdefault(k, v)
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


# ---------- 링크 정규화 ----------

def normalize_link(link: str) -> Optional[str]:
    """표준 형태로 변환. 파싱 실패 시 None.

    반환 예:
      t.me/+abc  -> https://t.me/+abc
      t.me/User  -> https://t.me/user  (public username 소문자)
      @user      -> https://t.me/user
    """
    if not link:
        return None
    s = link.strip()
    if not s:
        return None

    # @username
    m = re.fullmatch(r"@([A-Za-z][A-Za-z0-9_]{3,})", s)
    if m:
        return f"https://t.me/{m.group(1).lower()}"

    # URL 형태
    m = re.fullmatch(r"https?://t\.me/(.+?)/?", s, re.IGNORECASE)
    if m:
        rest = m.group(1).strip()
        # 초대 해시: +xxx 또는 joinchat/xxx → 대소문자 유지
        if rest.startswith("+"):
            return f"https://t.me/{rest}"
        if rest.lower().startswith("joinchat/"):
            return f"https://t.me/{rest}"
        # public username → 소문자
        return f"https://t.me/{rest.lower()}"

    # 순수 문자열 username
    m = re.fullmatch(r"([A-Za-z][A-Za-z0-9_]{3,})", s)
    if m:
        return f"https://t.me/{m.group(1).lower()}"

    return None


def _dedup_key(normalized: str) -> str:
    """중복 비교용 키. public은 소문자, 초대 해시는 원형."""
    return normalized  # normalize_link가 이미 키 형태로 맞춰줌


# ---------- 그룹 CRUD ----------

def _existing_keys(state: dict[str, Any]) -> set[str]:
    keys = set()
    for g in state["groups"]:
        n = normalize_link(g["link"])
        if n:
            keys.add(_dedup_key(n))
    return keys


def add_group(link: str) -> Optional[dict[str, Any]]:
    """Add one group. Returns entry, or None if duplicate/invalid."""
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
    }
    state["groups"].append(entry)
    write_state(state)
    return entry


def add_groups_bulk(links: list[str]) -> tuple[int, int]:
    """여러 링크 한번에 추가. (added_count, skipped_count) 반환."""
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


def update_group(group_id: str, **fields: Any) -> None:
    state = read_state()
    for g in state["groups"]:
        if g["id"] == group_id:
            g.update(fields)
            break
    write_state(state)


def reset_to_pending(group_id: str) -> None:
    update_group(group_id, status="pending", error=None, joined_at=None)


# ---------- 시드 파일 로딩 ----------

def load_seed_links() -> list[str]:
    """seed_links.txt 에서 링크 추출 → 정규화 → 중복 제거 → 셔플."""
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


def seed_if_empty() -> int:
    """groups가 비어있으면 시드 로드. 추가된 개수 반환."""
    state = read_state()
    if state["groups"]:
        return 0
    links = load_seed_links()
    if not links:
        return 0
    added, _ = add_groups_bulk(links)
    return added


def import_seed_merge() -> tuple[int, int]:
    """수동 호출: 시드 파일의 새 링크만 추가 (기존 유지)."""
    links = load_seed_links()
    return add_groups_bulk(links)


# ---------- 통계 업데이트 ----------

def bump_join_counters(state: dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    if state["stats"].get("last_reset") != today:
        state["stats"]["joined_today"] = 0
        state["stats"]["last_reset"] = today
    if state["stats"].get("hour_bucket") != hour:
        state["stats"]["joined_this_hour"] = 0
        state["stats"]["hour_bucket"] = hour
    state["stats"]["joined_today"] += 1
    state["stats"]["joined_this_hour"] += 1
    state["stats"]["joins_since_break"] = state["stats"].get("joins_since_break", 0) + 1


def maybe_reset_counters(state: dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    if state["stats"].get("last_reset") != today:
        state["stats"]["joined_today"] = 0
        state["stats"]["last_reset"] = today
    if state["stats"].get("hour_bucket") != hour:
        state["stats"]["joined_this_hour"] = 0
        state["stats"]["hour_bucket"] = hour


def record_floodwait(state: dict[str, Any]) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    events = state["stats"].get("floodwait_events", [])
    events.append(now_iso)
    # 24시간 이내 이벤트만 유지
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    state["stats"]["floodwait_events"] = events


def prune_floodwait(state: dict[str, Any]) -> int:
    """오래된 floodwait 이벤트 제거하고 24h 내 카운트 반환."""
    events = state["stats"].get("floodwait_events", [])
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    events = [
        e for e in events
        if datetime.fromisoformat(e.replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    state["stats"]["floodwait_events"] = events
    return len(events)
