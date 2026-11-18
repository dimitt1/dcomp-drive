"""Loops em segundo plano do Controlador: limpeza de leases, detecção de falhas e re-replicação (RF5, RF7, RF8)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from src.common.config import Settings, get_settings
from src.common.logger import get_logger
from src.common.schemas import EntryStatus, NodeStatus, ReplicaStatus
from src.controller.allocator import (
    InsufficientStorageNodesError,
    get_eligible_disks,
    select_targets_for_block,
)
from src.controller.database import get_session_factory
from src.controller.models import Block, BlockReplica, Entry, StorageNode, utc_now
from src.controller.routes_metadata import (
    cleanup_expired_leases,
    delete_physical_replicas_on_nodes,
    evaluate_entry_health,
)

logger = get_logger("controller.monitor")


@dataclass
class ReplicationOrder:
    """Representa uma ordem de cópia nó-a-nó agendada pelo monitor de re-replicação."""

    block_id: UUID
    entry_id: UUID
    block_index: int
    size_bytes: int
    expected_hash: str
    source_node_id: str
    source_node_url: str
    source_disk_id: str
    target_node_id: str
    target_node_url: str
    target_disk_id: str
    replaced_draining_node_id: str | None = None
    replaced_draining_disk_id: str | None = None
    replaced_draining_host_url: str | None = None


def _ensure_utc(dt: datetime | None) -> datetime | None:
    """Garante timezone UTC explícito em objetos datetime carregados do banco."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def detect_node_failures(
    db: AsyncSession,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> dict[str, list[str]]:
    """Avalia o timeout dos heartbeats e marca nós como SUSPECT ou DEAD (RF5).

    Atualiza também o estado (SUB_REPLICATED, RECOVERING, CORRUPTED) dos arquivos
    que possuem blocos nos nós afetados.
    """
    cfg = settings or get_settings()
    current_time = now or utc_now()

    stmt_nodes = select(StorageNode).order_by(StorageNode.node_id)
    nodes = (await db.execute(stmt_nodes)).scalars().all()

    newly_suspect: list[str] = []
    newly_dead: list[str] = []
    affected_node_ids: set[str] = set()

    for node in nodes:
        if node.status not in (NodeStatus.ALIVE.value, NodeStatus.SUSPECT.value):
            continue

        last_hb = _ensure_utc(node.last_heartbeat)
        if last_hb is None:
            continue

        elapsed = (current_time - last_hb).total_seconds()
        previous_status = node.status

        if elapsed >= cfg.heartbeat_timeout_sec:
            if node.status != NodeStatus.DEAD.value:
                node.status = NodeStatus.DEAD.value
                newly_dead.append(node.node_id)
                affected_node_ids.add(node.node_id)
                logger.warning(
                    "NODE_DEAD",
                    target_id=node.node_id,
                    node_id=node.node_id,
                    machine_id=node.machine_id,
                    previous_status=previous_status,
                    elapsed_sec=round(elapsed, 2),
                    timeout_sec=cfg.heartbeat_timeout_sec,
                )
        elif elapsed >= cfg.suspect_timeout_sec:
            if node.status == NodeStatus.ALIVE.value:
                node.status = NodeStatus.SUSPECT.value
                newly_suspect.append(node.node_id)
                affected_node_ids.add(node.node_id)
                logger.warning(
                    "NODE_SUSPECT",
                    target_id=node.node_id,
                    node_id=node.node_id,
                    machine_id=node.machine_id,
                    previous_status=previous_status,
                    elapsed_sec=round(elapsed, 2),
                    suspect_timeout_sec=cfg.suspect_timeout_sec,
                )

    if affected_node_ids:
        await db.flush()
        stmt_entries = (
            select(Entry)
            .join(Block, Block.entry_id == Entry.entry_id)
            .join(BlockReplica, BlockReplica.block_id == Block.block_id)
            .options(
                selectinload(Entry.blocks)
                .selectinload(Block.replicas)
                .selectinload(BlockReplica.node)
            )
            .where(
                Entry.is_dir.is_(False),
                Entry.status != EntryStatus.WRITING.value,
                BlockReplica.node_id.in_(affected_node_ids),
            )
            .distinct()
        )
        affected_entries = (await db.execute(stmt_entries)).scalars().all()
        for entry in affected_entries:
            new_health = evaluate_entry_health(entry).value
            if entry.status != new_health:
                entry.status = new_health
                entry.updated_at = current_time

        await db.commit()

    return {
        "newly_suspect": newly_suspect,
        "newly_dead": newly_dead,
    }


async def schedule_re_replications(
    db: AsyncSession,
    settings: Settings | None = None,
) -> tuple[list[ReplicationOrder], list[tuple[UUID, str, str]]]:
    """Identifica blocos sub-replicados ou em nós DRAINING e agenda ordens de cópia priorizadas (RF5, RF7).

    Prioridade:
    1. Blocos com menor número de réplicas `VERIFIED` em nós `ALIVE` (ex: 1 réplica viva antes de 2).
    2. Blocos com perda de réplica (nó DEAD ou escrita sub-replicada) antes de migração planejada (DRAINING).
    3. Maior déficit em relação a `replication_target`.
    4. Arquivos mais antigos (`created_at`) e menor `block_index`.
    """
    cfg = settings or get_settings()
    now = utc_now()

    stmt_active = (
        select(func.count())
        .select_from(BlockReplica)
        .where(BlockReplica.status == ReplicaStatus.COPYING.value)
    )
    active_copying = (await db.execute(stmt_active)).scalar_one() or 0
    available_slots = max(0, cfg.max_concurrent_replications - active_copying)

    stmt_blocks = (
        select(Block)
        .join(Entry, Block.entry_id == Entry.entry_id)
        .options(
            selectinload(Block.entry)
            .selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node),
            selectinload(Block.replicas).selectinload(BlockReplica.node),
        )
        .where(
            Entry.is_dir.is_(False),
            Entry.status != EntryStatus.WRITING.value,
        )
    )
    all_blocks = (await db.execute(stmt_blocks)).scalars().all()

    obsolete_draining_to_delete: list[tuple[UUID, str, str]] = []
    prioritized_candidates: list[
        tuple[
            tuple[int, int, int, datetime, int],
            Block,
            int,
            list[BlockReplica],
            list[BlockReplica],
            list[BlockReplica],
        ]
    ] = []
    status_dirty = False

    for block in all_blocks:
        entry = block.entry
        if entry is None or entry.is_dir or entry.status == EntryStatus.WRITING.value:
            continue

        # Evita agendar nova cópia para um bloco que já está em transferência
        if any(
            r.status in (ReplicaStatus.COPYING.value, ReplicaStatus.PENDING.value)
            for r in block.replicas
        ):
            continue

        alive_verified: list[BlockReplica] = []
        suspect_verified: list[BlockReplica] = []
        draining_verified: list[BlockReplica] = []
        dead_or_corrupted: list[BlockReplica] = []

        for rep in block.replicas:
            if rep.status != ReplicaStatus.VERIFIED.value or rep.node is None:
                dead_or_corrupted.append(rep)
                continue

            node_st = rep.node.status
            if node_st == NodeStatus.ALIVE.value:
                alive_verified.append(rep)
            elif node_st == NodeStatus.SUSPECT.value:
                suspect_verified.append(rep)
            elif node_st == NodeStatus.DRAINING.value:
                draining_verified.append(rep)
            else:
                dead_or_corrupted.append(rep)

        target_n = entry.replication_target or cfg.replication_factor
        # Nós SUSPECT aguardam confirmação de timeout (DEAD) antes de disparar cópia de seus blocos
        effective_healthy_count = len(alive_verified) + len(suspect_verified)

        if effective_healthy_count >= target_n:
            # Se o bloco já possui N réplicas saudáveis em nós ALIVE, limpa eventuais réplicas DEAD ou DRAINING excedentes
            for stale in dead_or_corrupted:
                await db.delete(stale)
                status_dirty = True
            for drain_rep in draining_verified:
                if drain_rep.node and drain_rep.node.host_url:
                    obsolete_draining_to_delete.append(
                        (block.block_id, drain_rep.disk_id, drain_rep.node.host_url)
                    )
                await db.delete(drain_rep)
                status_dirty = True
            continue

        readable_sources = alive_verified + draining_verified
        if not readable_sources:
            if not suspect_verified and entry.status != EntryStatus.CORRUPTED.value:
                entry.status = EntryStatus.CORRUPTED.value
                entry.updated_at = now
                status_dirty = True
            continue

        missing_count = target_n - effective_healthy_count
        is_drain_only = 0 if (dead_or_corrupted or len(block.replicas) < target_n) else 1
        created_at = _ensure_utc(entry.created_at) or now

        priority_key = (
            len(alive_verified),
            is_drain_only,
            -missing_count,
            created_at,
            block.block_index,
        )
        prioritized_candidates.append(
            (
                priority_key,
                block,
                missing_count,
                readable_sources,
                dead_or_corrupted,
                draining_verified,
            )
        )

    if available_slots <= 0 or not prioritized_candidates:
        if status_dirty:
            await db.commit()
        return [], obsolete_draining_to_delete

    prioritized_candidates.sort(key=lambda item: item[0])
    eligible_disks = await get_eligible_disks(db, cfg)

    orders: list[ReplicationOrder] = []
    affected_entries: set[ Entry ] = set()

    for (
        _prio,
        block,
        missing_count,
        readable_sources,
        dead_or_corrupted,
        draining_verified,
    ) in prioritized_candidates:
        if available_slots <= 0:
            break

        needed_now = min(missing_count, available_slots)
        source_rep = readable_sources[0]
        assert source_rep.node is not None

        # Exclui nós/máquinas/discos que já possuem réplica ativa/viva/draining do bloco
        active_replicas = [r for r in block.replicas if r not in dead_or_corrupted]
        excluded_nodes = {r.node_id for r in active_replicas}
        excluded_disks = {r.disk_id for r in active_replicas}
        excluded_machines = {
            r.node.machine_id for r in active_replicas if r.node is not None
        }
        # Também evita alocar no próprio nó DEAD de onde a réplica caiu
        for dead_rep in dead_or_corrupted:
            excluded_nodes.add(dead_rep.node_id)
            excluded_disks.add(dead_rep.disk_id)

        try:
            targets, _ = select_targets_for_block(
                candidates=eligible_disks,
                block_size_bytes=block.size_bytes,
                replication_target=needed_now,
                settings=cfg,
                excluded_node_ids=excluded_nodes,
                excluded_machine_ids=excluded_machines,
                excluded_disk_ids=excluded_disks,
            )
        except InsufficientStorageNodesError:
            # Se ainda não há nó substituto disponível, garante que o arquivo reflita SUB_REPLICATED
            if block.entry and block.entry.status == EntryStatus.HEALTHY.value:
                block.entry.status = EntryStatus.SUB_REPLICATED.value
                block.entry.updated_at = now
                status_dirty = True
            continue

        dead_queue = list(dead_or_corrupted)
        drain_queue = list(draining_verified)

        for target in targets:
            if available_slots <= 0:
                break

            replaced_drain_node: str | None = None
            replaced_drain_disk: str | None = None
            replaced_drain_url: str | None = None

            if dead_queue:
                stale_rep = dead_queue.pop(0)
                await db.delete(stale_rep)
                await db.flush()
            elif drain_queue:
                drain_rep = drain_queue.pop(0)
                replaced_drain_node = drain_rep.node_id
                replaced_drain_disk = drain_rep.disk_id
                replaced_drain_url = drain_rep.node.host_url if drain_rep.node else None
                await db.delete(drain_rep)
                await db.flush()

            copying_rep = BlockReplica(
                block_id=block.block_id,
                node_id=target.node_id,
                disk_id=target.disk_id,
                status=ReplicaStatus.COPYING.value,
            )
            db.add(copying_rep)
            available_slots -= 1
            status_dirty = True

            if block.entry is not None:
                block.entry.status = EntryStatus.RECOVERING.value
                block.entry.updated_at = now
                affected_entries.add(block.entry)

            order = ReplicationOrder(
                block_id=block.block_id,
                entry_id=block.entry_id,
                block_index=block.block_index,
                size_bytes=block.size_bytes,
                expected_hash=block.block_hash,
                source_node_id=source_rep.node_id,
                source_node_url=source_rep.node.host_url,
                source_disk_id=source_rep.disk_id,
                target_node_id=target.node_id,
                target_node_url=target.host_url,
                target_disk_id=target.disk_id,
                replaced_draining_node_id=replaced_drain_node,
                replaced_draining_disk_id=replaced_drain_disk,
                replaced_draining_host_url=replaced_drain_url,
            )
            orders.append(order)
            logger.info(
                "REPLICATION_START",
                target_id=str(block.block_id),
                block_id=str(block.block_id),
                entry_id=str(block.entry_id),
                source_node_id=order.source_node_id,
                source_disk_id=order.source_disk_id,
                target_node_id=order.target_node_id,
                target_disk_id=order.target_disk_id,
            )

    if status_dirty or orders or affected_entries:
        await db.commit()

    return orders, obsolete_draining_to_delete


async def execute_replication_order(
    order: ReplicationOrder,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> bool:
    """Executa uma ordem de transferência nó-a-nó (`POST /blocks/replicate`) e atualiza o banco."""
    factory = session_factory or get_session_factory()
    replicate_url = f"{order.source_node_url.rstrip('/')}/blocks/replicate"
    payload = {
        "block_id": str(order.block_id),
        "source_disk_id": order.source_disk_id,
        "target_node_url": order.target_node_url,
        "target_disk_id": order.target_disk_id,
        "expected_hash": order.expected_hash,
    }

    success = False
    try:
        if http_client is not None:
            resp = await http_client.post(replicate_url, json=payload)
            success = resp.status_code in (200, 201)
            if not success:
                logger.warning(
                    "re_replication_http_error",
                    block_id=str(order.block_id),
                    source_node_id=order.source_node_id,
                    target_node_id=order.target_node_id,
                    status_code=resp.status_code,
                    response=resp.text,
                )
        else:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(replicate_url, json=payload)
                success = resp.status_code in (200, 201)
                if not success:
                    logger.warning(
                        "re_replication_http_error",
                        block_id=str(order.block_id),
                        source_node_id=order.source_node_id,
                        target_node_id=order.target_node_id,
                        status_code=resp.status_code,
                        response=resp.text,
                    )
    except Exception as exc:
        logger.warning(
            "re_replication_exception",
            block_id=str(order.block_id),
            source_node_id=order.source_node_id,
            target_node_id=order.target_node_id,
            error=str(exc),
        )

    now = utc_now()
    draining_to_cleanup: list[tuple[UUID, str, str]] = []

    async with factory() as session:
        stmt_rep = select(BlockReplica).where(
            BlockReplica.block_id == order.block_id,
            BlockReplica.disk_id == order.target_disk_id,
        )
        rep = (await session.execute(stmt_rep)).scalar_one_or_none()
        if rep is not None:
            if success:
                rep.status = ReplicaStatus.VERIFIED.value
                rep.updated_at = now
                logger.info(
                    "REPLICATION_END",
                    target_id=str(order.block_id),
                    block_id=str(order.block_id),
                    entry_id=str(order.entry_id),
                    source_node_id=order.source_node_id,
                    target_node_id=order.target_node_id,
                    target_disk_id=order.target_disk_id,
                )
                if (
                    order.replaced_draining_disk_id
                    and order.replaced_draining_host_url
                ):
                    draining_to_cleanup.append(
                        (
                            order.block_id,
                            order.replaced_draining_disk_id,
                            order.replaced_draining_host_url,
                        )
                    )
            else:
                await session.delete(rep)
                await session.flush()
                # Se a cópia falhou durante um drain, restaura a referência à réplica do nó DRAINING
                if (
                    order.replaced_draining_node_id
                    and order.replaced_draining_disk_id
                ):
                    stmt_check = select(BlockReplica).where(
                        BlockReplica.block_id == order.block_id,
                        BlockReplica.node_id == order.replaced_draining_node_id,
                    )
                    existing_drain_rep = (await session.execute(stmt_check)).scalar_one_or_none()
                    if existing_drain_rep is None:
                        session.add(
                            BlockReplica(
                                block_id=order.block_id,
                                node_id=order.replaced_draining_node_id,
                                disk_id=order.replaced_draining_disk_id,
                                status=ReplicaStatus.VERIFIED.value,
                            )
                        )

        stmt_entry = (
            select(Entry)
            .options(
                selectinload(Entry.blocks)
                .selectinload(Block.replicas)
                .selectinload(BlockReplica.node)
            )
            .where(Entry.entry_id == order.entry_id)
        )
        entry = (await session.execute(stmt_entry)).scalar_one_or_none()
        if entry is not None and not entry.is_dir and entry.status != EntryStatus.WRITING.value:
            entry.status = evaluate_entry_health(entry).value
            entry.updated_at = now

        await session.commit()

    if draining_to_cleanup:
        await delete_physical_replicas_on_nodes(draining_to_cleanup)

    return success


async def run_cluster_monitor_cycle(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    http_client: httpx.AsyncClient | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Executa um ciclo completo de monitoramento: detecção de falhas + fila de re-replicação."""
    cfg = settings or get_settings()
    factory = session_factory or get_session_factory()

    async with factory() as session:
        failure_summary = await detect_node_failures(session, cfg, now=now)
        orders, obsolete_draining = await schedule_re_replications(session, cfg)

    if obsolete_draining:
        await delete_physical_replicas_on_nodes(obsolete_draining)

    completed = 0
    failed = 0
    if orders:
        semaphore = asyncio.Semaphore(cfg.max_concurrent_replications)

        async def _bounded_exec(order: ReplicationOrder) -> bool:
            async with semaphore:
                return await execute_replication_order(
                    order,
                    session_factory=factory,
                    http_client=http_client,
                )

        results = await asyncio.gather(*(_bounded_exec(o) for o in orders))
        completed = sum(1 for r in results if r)
        failed = len(results) - completed

    return {
        "newly_suspect": failure_summary["newly_suspect"],
        "newly_dead": failure_summary["newly_dead"],
        "scheduled_replications": len(orders),
        "completed_replications": completed,
        "failed_replications": failed,
    }


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


async def cluster_monitor_loop(stop_event: asyncio.Event) -> None:
    """Rotina periódica que detecta nós SUSPECT/DEAD e executa a fila de re-replicação (RF5, RF7)."""
    settings = get_settings()

    while not stop_event.is_set():
        try:
            await run_cluster_monitor_cycle(settings=settings)
        except Exception as exc:
            logger.error("cluster_monitor_loop_error", error=str(exc))

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=float(settings.monitor_interval_sec),
            )
        except asyncio.TimeoutError:
            pass
