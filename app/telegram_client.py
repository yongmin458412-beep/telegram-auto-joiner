"""Telethon 클라이언트 & 입장 로직."""
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

_client: Optional[TelegramClient] = None


def _get_env(name: str, required: bool = True) -> str:
    v = os.environ.get(name, "")
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


async def get_client() -> TelegramClient:
    global _client
    if _client is None:
        api_id = int(_get_env("TELEGRAM_API_ID"))
        api_hash = _get_env("TELEGRAM_API_HASH")
        session_str = _get_env("TELEGRAM_SESSION")
        _client = TelegramClient(StringSession(session_str), api_id, api_hash)
        await _client.connect()
        if not await _client.is_user_authorized():
            raise RuntimeError(
                "Telegram session not authorized. Re-run scripts/generate_session.py."
            )
    return _client


async def disconnect_client() -> None:
    global _client
    if _client is not None:
        await _client.disconnect()
        _client = None


_PRIVATE_INVITE = re.compile(
    r"(?:https?://)?t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)/?$"
)
_PUBLIC_USERNAME = re.compile(
    r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,})/?$"
)
_BARE_USERNAME = re.compile(r"^@?([A-Za-z][A-Za-z0-9_]{3,})$")


def parse_link(link: str) -> tuple[str, str]:
    """Return (kind, value). kind in {'invite','username'}."""
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


async def join_group(link: str) -> tuple[bool, Optional[str], Optional[str], Optional[int]]:
    """Join a group by link.

    Returns: (ok, title, error_message, flood_wait_seconds)
    flood_wait_seconds is non-None only on FloodWaitError.
    """
    try:
        kind, value = parse_link(link)
    except ValueError as e:
        return False, None, str(e), None

    client = await get_client()
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
        return False, None, f"{type(e).__name__}: {e}", None
