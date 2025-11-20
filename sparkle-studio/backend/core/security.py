"""
Security utilities for Sparkle Studio.
Placeholder implementation - will be completed in Phase 8.
"""
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from .config import settings


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class TokenData(BaseModel):
    """Token data model."""
    username: Optional[str] = None
    scopes: list[str] = []


class User(BaseModel):
    """User model - placeholder for Phase 8."""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False
    scopes: list[str] = []


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generate password hash."""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[TokenData]:
    """Decode and verify JWT token."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
        token_scopes = payload.get("scopes", [])
        return TokenData(username=username, scopes=token_scopes)
    except JWTError:
        return None


# Placeholder user for development (Phase 1-7)
DEFAULT_DEV_USER = User(
    username="dev_user",
    email="dev@sparkle.studio",
    full_name="Development User",
    disabled=False,
    scopes=["read", "write", "admin"]
)
