from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.settings.db_settings import settings

# Sync version
engine = create_engine(
    url=settings.SYNC_DATABASE_URL, echo=True, pool_size=100, max_overflow=15
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
