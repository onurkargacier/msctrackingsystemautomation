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


def get_current_user(token: str = Cookie(default=None), db: Session = Depends(get_db)) -> User:
    if not token:
        raise RequiresLogin()
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise RequiresLogin()
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise RequiresLogin()
    return user
