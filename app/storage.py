"""groups.json 원자적 읽기/쓰기."""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DATA_FILE = DATA_DIR / "groups.json"

_lock = threading.Lock()


def _empty_state() -> dict[str, Any]:
    return {
        "groups": [],
        "stats": {"joined_today": 0, "last_reset": datetime.now(timezone.utc).date().isoformat()},
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


def read_state() -> dict[str, Any]:
    with _lock:
        _ensure_file()
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)


def write_state(state: dict[str, Any]) -> None:
    with _lock:
        _ensure_file()
        _atomic_write(state)


def add_group(link: str) -> dict[str, Any]:
    state = read_state()
    now = datetime.now(timezone.utc).isoformat()
    entry = {
        "id": uuid.uuid4().hex[:12],
        "link": link.strip(),
        "status": "pending",
        "added_at": now,
        "joined_at": None,
        "error": None,
        "title": None,
    }
    state["groups"].append(entry)
    write_state(state)
    return entry


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


def bump_joined_today(state: dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state["stats"].get("last_reset") != today:
        state["stats"]["joined_today"] = 0
        state["stats"]["last_reset"] = today
    state["stats"]["joined_today"] += 1


def maybe_reset_daily(state: dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state["stats"].get("last_reset") != today:
        state["stats"]["joined_today"] = 0
        state["stats"]["last_reset"] = today
