# Telegram Auto Joiner

텔레그램 그룹 초대 링크 목록을 웹 UI로 관리하고, 자동으로 그룹에 입장시키는 FastAPI 앱입니다. Railway에서 24시간 돌아가도록 설계되었습니다.

## 주요 기능

- 웹 대시보드에서 초대 링크 추가/삭제/재시도
- 사용자 계정(Telethon) 기반 자동 입장 — `t.me/+xxx`, `t.me/joinchat/xxx`, `@username` 모두 지원
- 레이트 제한: 기본 30–60초 간격, 하루 20건 상한, `FloodWait` 자동 대기
- JSON 파일 기반 영속성 (Railway Volume 권장)
- HTTP Basic Auth 대시보드 보호 (선택)

## 로컬 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1) https://my.telegram.org 에서 API ID/Hash 발급
export TELEGRAM_API_ID=xxxxx
export TELEGRAM_API_HASH=xxxxxxxxxxxxxxxx

# 2) 세션 발급 (1회만) - 전화번호/OTP 입력
python scripts/generate_session.py
# 출력된 문자열을 TELEGRAM_SESSION으로 저장
export TELEGRAM_SESSION="1Aabc..."

# 3) 서버 실행
uvicorn app.main:app --reload
```

http://localhost:8000 접속.

## Railway 배포

1. 이 레포를 GitHub에 push
2. Railway → **New Project → Deploy from GitHub repo** → 이 레포 선택
3. **Variables** 탭에서 환경변수 설정:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `TELEGRAM_SESSION` (로컬에서 발급한 StringSession)
   - `DASHBOARD_PASSWORD` (대시보드 비밀번호, 강력히 권장)
   - `DAILY_JOIN_LIMIT`, `JOIN_DELAY_MIN`, `JOIN_DELAY_MAX` (선택)
4. **Settings → Volumes** 에서 Volume 추가, 마운트 경로 `/app/data`
5. 배포 완료 후 Railway가 제공하는 URL로 접속

## 주의사항

- `TELEGRAM_SESSION`은 계정 전체 권한입니다. **절대 Git에 커밋 금지.**
- 텔레그램은 과도한 그룹 가입을 계정 제한 사유로 봅니다. 하루 20건 권장값입니다.
- public 레포이므로 푸시 전 `git status`로 세션/env 파일 제외 확인하세요.
