"""Telethon 다중 계정 클라이언트 & 입장 로직."""
from __future__ import annotations

import os
import re
from typing import Optional

import asyncio

from telethon import TelegramClient
from telethon.errors import (
    ChannelsTooMuchError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    ChatForwardsRestrictedError,
    ChatWriteForbiddenError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    MessageIdInvalidError,
    SlowModeWaitError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
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


# ---------- Forward 기능 ----------

async def resolve_source_channel(
    account_name: str, source_link: str
) -> tuple[Optional[int], bool, Optional[str]]:
    """소스 채널을 해당 계정으로 해소(이미 멤버면 get_entity, 아니면 가입).

    Returns: (chat_id, accessible, error)
    """
    if not source_link:
        return None, False, "empty source_link"

    try:
        kind, value = parse_link(source_link)
    except ValueError as e:
        return None, False, f"bad link: {e}"

    client = await get_client(account_name)
    # 이미 멤버인지 시도
    try:
        entity = await client.get_entity(source_link)
        chat_id = getattr(entity, "id", None)
        return chat_id, True, None
    except Exception:
        pass

    # 가입 시도
    try:
        if kind == "invite":
            await client(ImportChatInviteRequest(value))
        else:
            await client(JoinChannelRequest(value))
        # 가입 후 다시 해소
        entity = await client.get_entity(source_link)
        return getattr(entity, "id", None), True, None
    except UserAlreadyParticipantError:
        try:
            entity = await client.get_entity(source_link)
            return getattr(entity, "id", None), True, None
        except Exception as e:
            return None, False, f"resolve after join: {e}"
    except FloodWaitError as e:
        return None, False, f"flood wait {e.seconds}s"
    except Exception as e:
        return None, False, f"{type(e).__name__}: {e}"


async def forward_and_counter(
    account_name: str,
    target_link: str,
    source_link: str,
    source_message_id: int,
    counter_text: str,
) -> tuple[bool, Optional[str], Optional[int], bool]:
    """원본 메시지 전달 + 카운터 메시지 전송.

    Returns: (ok, error, flood_wait_seconds, disable_group)
    disable_group=True 이면 해당 그룹은 쓰기 금지/차단 상태이므로
    forward_enabled 를 자동으로 false 로 내려야 함.
    """
    client = await get_client(account_name)

    try:
        target_entity = await client.get_entity(target_link)
    except FloodWaitError as e:
        return False, f"flood wait {e.seconds}s (resolve target)", int(e.seconds), False
    except Exception as e:
        return False, f"resolve target: {type(e).__name__}: {e}", None, False

    # 소스 채널을 링크(문자열)로 해석 — 재배포 후 캐시 유실 대비
    try:
        source_entity = await client.get_entity(source_link)
    except FloodWaitError as e:
        return False, f"flood wait {e.seconds}s (resolve source)", int(e.seconds), False
    except Exception as e:
        return False, f"resolve source: {type(e).__name__}: {e}", None, False

    # 1) 원본 메시지 전달
    try:
        await client.forward_messages(
            entity=target_entity,
            messages=source_message_id,
            from_peer=source_entity,
        )
    except FloodWaitError as e:
        return False, f"flood wait {e.seconds}s (forward)", int(e.seconds), False
    except (ChatWriteForbiddenError, UserBannedInChannelError, ChatAdminRequiredError):
        return False, "write forbidden / banned / admin required", None, True
    except ChatForwardsRestrictedError:
        return False, "target group has forwarding restricted", None, True
    except SlowModeWaitError as e:
        return False, f"slow mode {e.seconds}s", int(e.seconds), False
    except MessageIdInvalidError:
        return False, "source message id invalid", None, False
    except ChannelPrivateError:
        return False, "source channel private", None, False
    except Exception as e:
        err_name = type(e).__name__
        # Restricted/Forbidden 계열은 전부 영구 실패 → 삭제 대상
        if "Restricted" in err_name or "Forbidden" in err_name or "Admin" in err_name:
            return False, f"{err_name}: {e}", None, True
        return False, f"forward: {err_name}: {e}", None, False

    # 2) 짧은 대기 후 카운터 메시지
    await asyncio.sleep(1.5)
    try:
        await client.send_message(target_entity, counter_text)
    except FloodWaitError as e:
        # forward는 이미 성공했으므로 ok=True, 단 카운터는 미전송
        return True, f"forwarded, counter flood wait {e.seconds}s", int(e.seconds), False
    except (ChatWriteForbiddenError, UserBannedInChannelError):
        return True, "forwarded, counter write forbidden", None, False
    except SlowModeWaitError as e:
        return True, f"forwarded, counter slow mode {e.seconds}s", int(e.seconds), False
    except Exception as e:
        return True, f"forwarded, counter: {type(e).__name__}: {e}", None, False

    return True, None, None, False


async def send_direct_and_counter(
    account_name: str,
    target_link: str,
    message_text: str,
    counter_text: str,
) -> tuple[bool, Optional[str], Optional[int], bool]:
    """직접 텍스트 메시지 전송 + 카운터 메시지.

    forward 대신 send_message 사용 — forward 제한에 영향 안 받음.
    Returns: (ok, error, flood_wait_seconds, disable_group)
    """
    client = await get_client(account_name)

    try:
        target_entity = await client.get_entity(target_link)
    except FloodWaitError as e:
        return False, f"flood wait {e.seconds}s (resolve target)", int(e.seconds), False
    except Exception as e:
        return False, f"resolve target: {type(e).__name__}: {e}", None, False

    # 1) 직접 메시지 전송
    try:
        await client.send_message(target_entity, message_text)
    except FloodWaitError as e:
        return False, f"flood wait {e.seconds}s (send)", int(e.seconds), False
    except (ChatWriteForbiddenError, UserBannedInChannelError, ChatAdminRequiredError):
        return False, "write forbidden / banned / admin required", None, True
    except ChatForwardsRestrictedError:
        return False, "target group restricted", None, True
    except SlowModeWaitError as e:
        return False, f"slow mode {e.seconds}s", int(e.seconds), False
    except Exception as e:
        err_name = type(e).__name__
        if "Restricted" in err_name or "Forbidden" in err_name or "Admin" in err_name:
            return False, f"{err_name}: {e}", None, True
        return False, f"send: {err_name}: {e}", None, False

    # 2) 짧은 대기 후 카운터 메시지
    await asyncio.sleep(1.5)
    try:
        await client.send_message(target_entity, counter_text)
    except FloodWaitError as e:
        return True, f"sent, counter flood wait {e.seconds}s", int(e.seconds), False
    except (ChatWriteForbiddenError, UserBannedInChannelError, ChatAdminRequiredError):
        return True, "sent, counter write forbidden", None, False
    except SlowModeWaitError as e:
        return True, f"sent, counter slow mode {e.seconds}s", int(e.seconds), False
    except Exception as e:
        return True, f"sent, counter: {type(e).__name__}: {e}", None, False

    return True, None, None, False
