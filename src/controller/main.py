"""Aplicação principal FastAPI do Controlador + Nó de Metadados."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.common.logger import get_logger
from src.controller.database import close_db, init_db
from src.controller.monitor import cluster_monitor_loop, lease_cleanup_loop
from src.controller.routes_cluster import router as cluster_router
from src.controller.routes_metadata import router as metadata_router
from src.controller.web_ui import router as web_ui_router

logger = get_logger("controller.main")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Gerencia o ciclo de vida do Controlador: inicializa DB e tarefas em background."""
    await init_db()
    stop_event = asyncio.Event()
    lease_task = asyncio.create_task(lease_cleanup_loop(stop_event))
    monitor_task = asyncio.create_task(cluster_monitor_loop(stop_event))
    logger.info("controller_started")
    try:
        yield
    finally:
        stop_event.set()
        lease_task.cancel()
        monitor_task.cancel()
        for task in (lease_task, monitor_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await close_db()
        logger.info("controller_stopped")


app = FastAPI(
    title="DComp Drive — Controlador e Nó de Metadados",
    version="1.0.0",
    description="Plano de Controle e Metadados do Sistema Distribuído de Armazenamento de Arquivos.",
    lifespan=lifespan,
)

app.include_router(metadata_router)
app.include_router(cluster_router)
app.include_router(web_ui_router)


@app.get("/health", tags=["Health"])
async def healthcheck() -> dict[str, str]:
    """Endpoint simples de verificação de saúde do Controlador."""
    return {"status": "ok", "component": "controller"}
