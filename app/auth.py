from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from fastapi import Cookie, Depends
from sqlalchemy.orm import Session
from app.database import get_db, User
import os

SECRET_KEY = os.environ.get("SECRET_KEY", "change-this-in-production-very-long-secret-key-12345")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)


def create_token(user_id: int) -> str:
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    return jwt.encode({"sub": str(user_id), "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


class RequiresLogin(Exception):
    pass


class RequiresAdmin(Exception):
    pass


def _decode_user(token: str | None, db: Session) -> User | None:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        return None
    return db.query(User).filter(User.id == user_id, User.is_active == True).first()


def get_current_user(token: str = Cookie(default=None), db: Session = Depends(get_db)) -> User:
    user = _decode_user(token, db)
    if not user:
        raise RequiresLogin()
    return user


def get_current_admin(token: str = Cookie(default=None), db: Session = Depends(get_db)) -> User:
    user = _decode_user(token, db)
    if not user:
        raise RequiresLogin()
    if not user.is_admin:
        raise RequiresAdmin()
    return user
