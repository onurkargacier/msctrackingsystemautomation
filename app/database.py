from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./app.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    hashed_password = Column(String(256), nullable=False)
    bl_numbers = relationship("BLNumber", back_populates="user", cascade="all, delete")
    jobs = relationship("Job", back_populates="user", cascade="all, delete")


class BLNumber(Base):
    __tablename__ = "bl_numbers"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    bl = Column(String(50), nullable=False)
    user = relationship("User", back_populates="bl_numbers")


class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(String(20), default="running")  # running, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    user = relationship("User", back_populates="jobs")
    results = relationship("Result", back_populates="job", cascade="all, delete")


class Result(Base):
    __tablename__ = "results"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    bl = Column(String(50))
    eta = Column(String(100))
    etd = Column(String(100))
    kaynak = Column(String(100))
    log = Column(Text)
    job = relationship("Job", back_populates="results")


def create_tables():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
