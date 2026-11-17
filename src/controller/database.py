"""Configuração de Engine e Sessão Assíncrona SQLAlchemy 2.0 + PostgreSQL (Persistência RF10)."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.common.config import get_settings
from src.common.logger import get_logger
from src.common.schemas import EntryStatus
from src.controller.models import Base, Entry

logger = get_logger("controller.database")

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def configure_database(
    database_url: str | None = None,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Inicializa ou reconfigura o AsyncEngine e o async_sessionmaker para o PostgreSQL."""
    global _engine, _session_factory

    url = database_url or get_settings().database_url
    _engine = create_async_engine(
        url,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )

    _session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    return _engine, _session_factory


def get_engine() -> AsyncEngine:
    """Retorna o AsyncEngine atual, inicializando-o se necessário."""
    if _engine is None:
        configure_database()
    assert _engine is not None
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Retorna o async_sessionmaker atual, inicializando-o se necessário."""
    if _session_factory is None:
        configure_database()
    assert _session_factory is not None
    return _session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependência do FastAPI para fornecer uma sessão de banco de dados por requisição."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    """Cria tabelas no PostgreSQL (se não existirem) e garante o diretório raiz '/' (RF10)."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = get_session_factory()
    async with session_factory() as session:
        result = await session.execute(select(Entry).where(Entry.path == "/"))
        root_entry = result.scalar_one_or_none()
        if root_entry is None:
            root = Entry(
                path="/",
                parent_path="/",
                is_dir=True,
                size_bytes=0,
                replication_target=get_settings().replication_factor,
                is_encrypted=False,
                status=EntryStatus.DIRECTORY.value,
            )
            session.add(root)
            await session.commit()
            logger.info("root_directory_initialized", path="/")

    logger.info("database_initialized")


async def close_db() -> None:
    """Fecha conexões do pool do banco de dados no encerramento do Controlador."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("database_closed")
