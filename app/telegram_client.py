"""Telethon 다중 계정 클라이언트 & 입장 로직."""
from __future__ import annotations

import os
import re
from typing import Optional

from telethon import TelegramClient
from telethon.errors import (
    ChannelsTooMuchError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

try:
    from telethon.errors import InviteRequestSentError  # type: ignore
except ImportError:
    InviteRequestSentError = None  # type: ignore


_clients: dict[str, TelegramClient] = {}


def _get_env(name: str, required: bool = True) -> str:
    v = os.environ.get(name, "")
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def discover_accounts() -> list[tuple[str, str]]:
    """환경변수에서 계정 발견.

    Returns list of (account_name, session_string).

    지원 형태:
      - TELEGRAM_SESSION (단일) -> account name "1"
      - TELEGRAM_SESSION_1, TELEGRAM_SESSION_2, ... (다중)
    """
    accounts: list[tuple[str, str]] = []
    found_names: set[str] = set()

    # 번호 붙은 형태 먼저
    pattern = re.compile(r"^TELEGRAM_SESSION_(.+)$")
    for key, val in os.environ.items():
        m = pattern.match(key)
        if m and val.strip():
            name = m.group(1)
            accounts.append((name, val.strip()))
            found_names.add(name)

    # legacy 단일 세션
    legacy = os.environ.get("TELEGRAM_SESSION", "").strip()
    if legacy and "1" not in found_names:
        accounts.append(("1", legacy))

    # 이름으로 정렬 (1, 2, 3...)
    def sort_key(p: tuple[str, str]):
        name = p[0]
        try:
            return (0, int(name))
        except ValueError:
            return (1, name)

    accounts.sort(key=sort_key)
    return accounts


def _get_account_credentials(account_name: str) -> tuple[int, str]:
    """계정별 api_id/hash 우선, 없으면 전역 fallback."""
    per_id = os.environ.get(f"TELEGRAM_API_ID_{account_name}", "").strip()
    per_hash = os.environ.get(f"TELEGRAM_API_HASH_{account_name}", "").strip()
    if per_id and per_hash:
        return int(per_id), per_hash
    return int(_get_env("TELEGRAM_API_ID")), _get_env("TELEGRAM_API_HASH")


async def get_client(account_name: str) -> TelegramClient:
    if account_name in _clients:
        return _clients[account_name]
    api_id, api_hash = _get_account_credentials(account_name)
    sessions = dict(discover_accounts())
    if account_name not in sessions:
        raise RuntimeError(f"Unknown account: {account_name}")
    client = TelegramClient(StringSession(sessions[account_name]), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            f"Session '{account_name}' not authorized. Re-run generate_session.py."
        )
    _clients[account_name] = client
    return client


async def disconnect_all() -> None:
    for name, client in list(_clients.items()):
        try:
            await client.disconnect()
        except Exception:
            pass
    _clients.clear()


_PRIVATE_INVITE = re.compile(
    r"(?:https?://)?t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)/?$"
)
_PUBLIC_USERNAME = re.compile(
    r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,})/?$"
)
_BARE_USERNAME = re.compile(r"^@?([A-Za-z][A-Za-z0-9_]{3,})$")


def parse_link(link: str) -> tuple[str, str]:
    link = link.strip()
    m = _PRIVATE_INVITE.match(link)
    if m:
        return "invite", m.group(1)
    m = _PUBLIC_USERNAME.match(link)
    if m:
        return "username", m.group(1)
    m = _BARE_USERNAME.match(link)
    if m:
        return "username", m.group(1)
    raise ValueError(f"Unrecognized Telegram link: {link}")


async def join_group(account_name: str, link: str) -> tuple[bool, Optional[str], Optional[str], Optional[int]]:
    """Join a group using the specified account.

    Returns: (ok, title, error_message, flood_wait_seconds)
    """
    try:
        kind, value = parse_link(link)
    except ValueError as e:
        return False, None, str(e), None

    client = await get_client(account_name)
    try:
        if kind == "invite":
            updates = await client(ImportChatInviteRequest(value))
            title = None
            chats = getattr(updates, "chats", []) or []
            if chats:
                title = getattr(chats[0], "title", None)
            return True, title, None, None
        else:
            result = await client(JoinChannelRequest(value))
            title = None
            chats = getattr(result, "chats", []) or []
            if chats:
                title = getattr(chats[0], "title", None)
            return True, title, None, None
    except UserAlreadyParticipantError:
        return True, None, "already a member", None
    except InviteHashExpiredError:
        return False, None, "invite link expired", None
    except InviteHashInvalidError:
        return False, None, "invite link invalid", None
    except ChannelsTooMuchError:
        return False, None, "account joined too many channels", None
    except FloodWaitError as e:
        return False, None, f"flood wait {e.seconds}s", int(e.seconds)
    except Exception as e:
        if InviteRequestSentError is not None and isinstance(e, InviteRequestSentError):
            return True, None, "join request sent (approval pending)", None
        if "InviteRequestSent" in type(e).__name__ or "successfully requested" in str(e):
            return True, None, "join request sent (approval pending)", None
        return False, None, f"{type(e).__name__}: {e}", None
