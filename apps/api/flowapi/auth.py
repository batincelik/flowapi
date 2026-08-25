import hashlib
import hmac
import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from argon2.low_level import Type
from fastapi import Cookie, Depends, Header, HTTPException, Response, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .database import session
from .models import AuditEvent, Session, User

SESSION_COOKIE = "flowapi_session"
_hasher = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=4, hash_len=32, salt_len=16, type=Type.ID)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def hash_password(password: str) -> str:
    if len(password) < 12:
        raise ValueError("Password must contain at least 12 characters")
    return _hasher.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, InvalidHashError):
        return False


async def create_first_admin(db: AsyncSession, email: str, password: str) -> User:
    normalized = email.strip().lower()
    password_hash = hash_password(password)
    async with db.begin():
        # Serializes first-run setup across API replicas.
        await db.execute(text("SELECT pg_advisory_xact_lock(731948201)"))
        if (await db.scalar(select(func.count()).select_from(User))) != 0:
            raise PermissionError("Initial setup has already completed")
        user = User(email=normalized, password_hash=password_hash, is_admin=True)
        db.add(user)
        await db.flush()
        db.add(
            AuditEvent(
                actor_user_id=user.id,
                event_type="instance.admin_created",
                target_type="user",
                target_id=str(user.id),
            )
        )
    return user


async def issue_session(db: AsyncSession, user: User, settings: Settings) -> tuple[str, str, Session]:
    raw_token, csrf = secrets.token_urlsafe(48), secrets.token_urlsafe(32)
    record = Session(
        user_id=user.id,
        token_hash=token_hash(raw_token),
        csrf_hash=token_hash(csrf),
        expires_at=datetime.now(UTC) + timedelta(hours=settings.SESSION_TTL_HOURS),
    )
    db.add(record)
    await db.commit()
    return raw_token, csrf, record


def set_session_cookie(response: Response, token: str, settings: Settings) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        secure=settings.SESSION_COOKIE_SECURE,
        samesite="lax",
        max_age=settings.SESSION_TTL_HOURS * 3600,
        path="/",
    )


async def authenticated_session(
    db: Annotated[AsyncSession, Depends(session)],
    raw_token: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> Session:
    if raw_token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    record = await db.scalar(
        select(Session).where(
            Session.token_hash == token_hash(raw_token),
            Session.revoked_at.is_(None),
            Session.expires_at > datetime.now(UTC),
        )
    )
    if record is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired session")
    return record


async def current_user(
    db: Annotated[AsyncSession, Depends(session)],
    record: Annotated[Session, Depends(authenticated_session)],
) -> User:
    user = await db.get(User, record.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session user no longer exists")
    # Authentication is read-only. End its implicit transaction so endpoint services can
    # open explicit atomic transactions on the same request-scoped session.
    await db.commit()
    return user


async def require_csrf(
    record: Annotated[Session, Depends(authenticated_session)],
    csrf: Annotated[str | None, Header(alias="X-CSRF-Token")] = None,
) -> None:
    if csrf is None or not hmac.compare_digest(record.csrf_hash, token_hash(csrf)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF validation failed")


async def authenticate(db: AsyncSession, email: str, password: str) -> User | None:
    user = await db.scalar(select(User).where(User.email == email.strip().lower()))
    if user is None or not verify_password(user.password_hash, password):
        return None
    return user


async def revoke_session(db: AsyncSession, record: Session) -> None:
    record.revoked_at = datetime.now(UTC)
    await db.commit()
