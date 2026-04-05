"""로컬에서 1회 실행 → StringSession 발급.

사용법:
    export TELEGRAM_API_ID=xxxxx
    export TELEGRAM_API_HASH=xxxxxxxxxxxxxxxx
    python scripts/generate_session.py

전화번호 / OTP / (설정된 경우) 2FA 비밀번호를 입력하면
세션 문자열이 출력됩니다. 이 문자열을 Railway 환경변수
TELEGRAM_SESSION 에 저장하세요.
"""
from __future__ import annotations

import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession


def main() -> None:
    api_id = os.environ.get("TELEGRAM_API_ID")
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        print("ERROR: TELEGRAM_API_ID / TELEGRAM_API_HASH 환경변수를 먼저 설정하세요.")
        print("발급: https://my.telegram.org")
        sys.exit(1)

    print("=" * 60)
    print("Telegram 세션 발급")
    print("=" * 60)
    print("전화번호는 국가코드 포함 국제표기로 입력하세요 (예: +821012345678)")
    print()

    with TelegramClient(StringSession(), int(api_id), api_hash) as client:
        session_str = client.session.save()
        print()
        print("=" * 60)
        print("세션 생성 완료! 아래 문자열을 TELEGRAM_SESSION 환경변수에 저장하세요:")
        print("=" * 60)
        print(session_str)
        print("=" * 60)
        print("주의: 이 문자열은 비밀번호와 동급입니다. 절대 공개하지 마세요.")


if __name__ == "__main__":
    main()
