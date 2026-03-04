import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.ext.declarative import declarative_base


DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_DATABASE = os.getenv("POSTGRES_DB", "marketplace")
ASYNC_DB_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}"

engine = create_async_engine(
    ASYNC_DB_URL,
    echo=True,
    pool_pre_ping=True
)

get_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()
