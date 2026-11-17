"""Loops em segundo plano do Controlador: limpeza de leases expirados e monitor de nós."""

from __future__ import annotations

import asyncio

from src.common.config import get_settings
from src.common.logger import get_logger
from src.controller.database import get_session_factory
from src.controller.routes_metadata import (
    cleanup_expired_leases,
    delete_physical_replicas_on_nodes,
)

logger = get_logger("controller.monitor")


async def lease_cleanup_loop(stop_event: asyncio.Event) -> None:
    """Rotina periódica que limpa leases de escrita expirados (RF8)."""
    settings = get_settings()
    session_factory = get_session_factory()

    while not stop_event.is_set():
        try:
            async with session_factory() as session:
                orphans = await cleanup_expired_leases(session)
            if orphans:
                await delete_physical_replicas_on_nodes(orphans)
        except Exception as exc:
            logger.error("lease_cleanup_loop_error", error=str(exc))

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=float(settings.lease_cleanup_interval_sec),
            )
        except asyncio.TimeoutError:
            pass
