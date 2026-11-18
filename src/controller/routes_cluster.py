"""Rotas de Cluster, Descoberta, Heartbeat, Espaço e Observabilidade (RF5, RF6, RF7, RF11)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.common.config import get_settings
from src.common.logger import get_logger
from src.common.schemas import (
    EntryStatus,
    NodeHeartbeatRequest,
    NodeRegisterRequest,
    NodeStatus,
    ReplicaStatus,
)
from src.controller.database import get_db
from src.controller.models import Block, BlockReplica, Entry, NodeDisk, StorageNode, utc_now
from src.controller.routes_metadata import evaluate_entry_health

logger = get_logger("controller.cluster")

router = APIRouter(tags=["Cluster & Nodes"])


async def _refresh_entries_health(db: AsyncSession, entry_ids: set[UUID]) -> None:
    """Recalcula e atualiza o status dos arquivos afetados por mudanças de nós/réplicas."""
    if not entry_ids:
        return
    now = utc_now()
    stmt = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.entry_id.in_(entry_ids))
    )
    entries = (await db.execute(stmt)).scalars().all()
    for entry in entries:
        if entry.is_dir or entry.status == EntryStatus.WRITING.value:
            continue
        new_status = evaluate_entry_health(entry).value
        if entry.status != new_status:
            entry.status = new_status
            entry.updated_at = now


@router.post("/nodes/register", status_code=status.HTTP_200_OK)
async def register_node(
    request: NodeRegisterRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Registra ou reconcilia um Nó de Armazenamento no Controlador (RF6)."""
    settings = get_settings()
    now = utc_now()
    stmt = (
        select(StorageNode)
        .options(selectinload(StorageNode.disks))
        .where(StorageNode.node_id == request.node_id)
    )
    node = (await db.execute(stmt)).scalar_one_or_none()

    previous_status: str | None = None
    if node is None:
        node = StorageNode(
            node_id=request.node_id,
            host_url=request.host_url,
            machine_id=request.machine_id,
            status=NodeStatus.ALIVE.value,
            last_heartbeat=now,
        )
        db.add(node)
        await db.flush()
        existing_disks: dict[str, NodeDisk] = {}
    else:
        previous_status = node.status
        node.host_url = request.host_url
        node.machine_id = request.machine_id
        if node.status != NodeStatus.DRAINING.value:
            node.status = NodeStatus.ALIVE.value
        node.last_heartbeat = now
        existing_disks = {d.disk_id: d for d in node.disks}

    reported_disk_ids = {d_req.disk_id for d_req in request.disks}
    for d_req in request.disks:
        if d_req.disk_id in existing_disks:
            disk = existing_disks[d_req.disk_id]
            disk.mount_path = d_req.mount_path
            disk.total_bytes = d_req.total_bytes
            disk.used_bytes = d_req.used_bytes
            disk.is_active = d_req.is_active
        else:
            db.add(
                NodeDisk(
                    disk_id=d_req.disk_id,
                    node_id=node.node_id,
                    mount_path=d_req.mount_path,
                    total_bytes=d_req.total_bytes,
                    used_bytes=d_req.used_bytes,
                    is_active=d_req.is_active,
                )
            )

    for disk_id, disk in existing_disks.items():
        if disk_id not in reported_disk_ids:
            disk.is_active = False

    await db.flush()

    # Reconciliação de blocos ao retornar após falha (RF6)
    obsolete_blocks: dict[str, list[str]] = {}
    reconciled_count = 0
    affected_entries: set[UUID] = set()

    # 1. Remove metadados de réplicas que estavam registradas para este nó mas não
    # constam mais no inventário físico enviado pelo nó (ex: perda de disco/arquivo).
    reported_pairs: set[tuple[str, UUID]] = {
        (disk_id, bid)
        for disk_id, block_ids in request.existing_blocks.items()
        for bid in block_ids
    }
    stmt_node_reps = (
        select(BlockReplica)
        .options(selectinload(BlockReplica.block).selectinload(Block.entry))
        .where(BlockReplica.node_id == request.node_id)
    )
    node_reps = (await db.execute(stmt_node_reps)).scalars().all()
    for rep in node_reps:
        if (rep.disk_id, rep.block_id) not in reported_pairs:
            # Preserva apenas se o arquivo estiver em meio a uma alocação ativa (WRITING)
            if (
                rep.status == ReplicaStatus.PENDING.value
                and rep.block is not None
                and rep.block.entry is not None
                and rep.block.entry.status == EntryStatus.WRITING.value
            ):
                continue
            if rep.block is not None:
                affected_entries.add(rep.block.entry_id)
            await db.delete(rep)

    await db.flush()

    # 2. Reconcilia cada bloco reportado pelo nó contra os metadados atuais
    for disk_id, block_ids in request.existing_blocks.items():
        if disk_id not in reported_disk_ids:
            obsolete_blocks.setdefault(disk_id, []).extend(str(bid) for bid in block_ids)
            continue

        for bid in block_ids:
            stmt_blk = (
                select(Block)
                .options(
                    selectinload(Block.entry),
                    selectinload(Block.replicas).selectinload(BlockReplica.node),
                )
                .where(Block.block_id == bid)
            )
            blk = (await db.execute(stmt_blk)).scalar_one_or_none()
            if blk is None or blk.entry is None:
                obsolete_blocks.setdefault(disk_id, []).append(str(bid))
                continue

            affected_entries.add(blk.entry_id)
            target_n = blk.entry.replication_target or settings.replication_factor

            # Verifica réplicas VERIFIED já ativas em outros nós vivos
            other_alive_verified = [
                r
                for r in blk.replicas
                if r.node_id != request.node_id
                and r.status == ReplicaStatus.VERIFIED.value
                and r.node is not None
                and r.node.status == NodeStatus.ALIVE.value
            ]
            other_machines = {
                r.node.machine_id for r in other_alive_verified if r.node is not None
            }

            stmt_rep = select(BlockReplica).where(
                BlockReplica.block_id == bid,
                BlockReplica.node_id == request.node_id,
            )
            rep = (await db.execute(stmt_rep)).scalar_one_or_none()

            if rep is not None:
                if rep.disk_id != disk_id or rep.status == ReplicaStatus.CORRUPTED.value:
                    if rep.status == ReplicaStatus.CORRUPTED.value:
                        await db.delete(rep)
                    obsolete_blocks.setdefault(disk_id, []).append(str(bid))
                    continue

                # Se o nó está DRAINING ou se o bloco já atingiu N réplicas em outras máquinas
                # enquanto este nó estava ausente, descarta a cópia excedente.
                if (
                    node.status != NodeStatus.DRAINING.value
                    and (
                        len(other_alive_verified) >= target_n
                        or request.machine_id in other_machines
                    )
                    and previous_status in (NodeStatus.DEAD.value, NodeStatus.SUSPECT.value)
                ):
                    await db.delete(rep)
                    obsolete_blocks.setdefault(disk_id, []).append(str(bid))
                    continue

                if blk.entry.status != EntryStatus.WRITING.value:
                    rep.status = ReplicaStatus.VERIFIED.value
                    rep.updated_at = now
                reconciled_count += 1
            else:
                # Aproveita réplica íntegra se o bloco ainda precisar de réplicas e respeitar não-repetição
                if (
                    node.status == NodeStatus.ALIVE.value
                    and blk.entry.status != EntryStatus.WRITING.value
                    and len(other_alive_verified) < target_n
                    and request.machine_id not in other_machines
                ):
                    db.add(
                        BlockReplica(
                            block_id=bid,
                            node_id=request.node_id,
                            disk_id=disk_id,
                            status=ReplicaStatus.VERIFIED.value,
                        )
                    )
                    reconciled_count += 1
                else:
                    obsolete_blocks.setdefault(disk_id, []).append(str(bid))

    await db.flush()
    await _refresh_entries_health(db, affected_entries)
    await db.commit()

    logger.info(
        "node_registered",
        node_id=node.node_id,
        machine_id=node.machine_id,
        previous_status=previous_status,
        status=node.status,
        disks_count=len(request.disks),
        reconciled_blocks=reconciled_count,
        obsolete_blocks_count=sum(len(v) for v in obsolete_blocks.values()),
    )
    return {
        "node_id": node.node_id,
        "status": node.status,
        "reconciled_blocks": reconciled_count,
        "obsolete_blocks": obsolete_blocks,
    }


@router.post("/nodes/heartbeat", status_code=status.HTTP_200_OK)
async def node_heartbeat(
    request: NodeHeartbeatRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Recebe heartbeat e atualiza uso de disco (RF5, RF7)."""
    now = utc_now()
    stmt = (
        select(StorageNode)
        .options(selectinload(StorageNode.disks))
        .where(StorageNode.node_id == request.node_id)
    )
    node = (await db.execute(stmt)).scalar_one_or_none()
    if node is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nó '{request.node_id}' não registrado.",
        )

    previous_status = node.status
    node.last_heartbeat = now
    if node.status in (NodeStatus.SUSPECT.value, NodeStatus.DEAD.value):
        node.status = NodeStatus.ALIVE.value

    disk_map = {d.disk_id: d for d in node.disks}
    for d_req in request.disks:
        if d_req.disk_id in disk_map:
            d = disk_map[d_req.disk_id]
            d.mount_path = d_req.mount_path
            d.total_bytes = d_req.total_bytes
            d.used_bytes = d_req.used_bytes
            d.is_active = d_req.is_active

    if previous_status in (NodeStatus.SUSPECT.value, NodeStatus.DEAD.value):
        stmt_entries = (
            select(Block.entry_id)
            .join(BlockReplica, BlockReplica.block_id == Block.block_id)
            .where(BlockReplica.node_id == node.node_id)
            .distinct()
        )
        entry_ids = set((await db.execute(stmt_entries)).scalars().all())
        await _refresh_entries_health(db, entry_ids)
        logger.info(
            "node_recovered",
            node_id=node.node_id,
            previous_status=previous_status,
            status=node.status,
        )

    await db.commit()
    return {"node_id": node.node_id, "status": node.status, "last_heartbeat": now.isoformat()}


@router.post("/nodes/{node_id}/drain", status_code=status.HTTP_200_OK)
async def drain_node(
    node_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Marca um nó como DRAINING para esvaziamento planejado (RF7)."""
    stmt = select(StorageNode).where(StorageNode.node_id == node_id)
    node = (await db.execute(stmt)).scalar_one_or_none()
    if node is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Nó '{node_id}' não encontrado.")

    node.status = NodeStatus.DRAINING.value
    await db.flush()

    stmt_entries = (
        select(Block.entry_id)
        .join(BlockReplica, BlockReplica.block_id == Block.block_id)
        .where(BlockReplica.node_id == node_id)
        .distinct()
    )
    entry_ids = set((await db.execute(stmt_entries)).scalars().all())
    await _refresh_entries_health(db, entry_ids)

    stmt_rep_count = (
        select(func.count())
        .select_from(BlockReplica)
        .where(
            BlockReplica.node_id == node_id,
            BlockReplica.status == ReplicaStatus.VERIFIED.value,
        )
    )
    replicas_to_migrate = (await db.execute(stmt_rep_count)).scalar_one() or 0

    await db.commit()
    logger.info(
        "node_draining_started",
        node_id=node_id,
        replicas_to_migrate=replicas_to_migrate,
    )
    return {
        "node_id": node_id,
        "status": node.status,
        "replicas_to_migrate": replicas_to_migrate,
    }


@router.get("/cluster/status", status_code=status.HTTP_200_OK)
async def get_cluster_status(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retorna visibilidade completa do estado do cluster (RF11)."""
    stmt_nodes = select(StorageNode).options(selectinload(StorageNode.disks)).order_by(StorageNode.node_id)
    nodes = (await db.execute(stmt_nodes)).scalars().all()

    stmt_entries = (
        select(Entry.status, func.count(Entry.entry_id))
        .where(Entry.is_dir.is_(False))
        .group_by(Entry.status)
    )
    entry_counts = dict((await db.execute(stmt_entries)).all())

    stmt_copying = select(func.count()).select_from(BlockReplica).where(
        BlockReplica.status == ReplicaStatus.COPYING.value
    )
    active_replications = (await db.execute(stmt_copying)).scalar_one() or 0

    nodes_by_status = {st.value: 0 for st in NodeStatus}
    total_bytes = 0
    used_bytes = 0
    for n in nodes:
        if n.status in nodes_by_status:
            nodes_by_status[n.status] += 1
        for d in n.disks:
            if d.is_active:
                total_bytes += d.total_bytes
                used_bytes += d.used_bytes

    free_bytes = max(0, total_bytes - used_bytes)
    usage_pct = round((used_bytes / total_bytes) * 100.0, 2) if total_bytes > 0 else 0.0

    return {
        "nodes": [
            {
                "node_id": n.node_id,
                "host_url": n.host_url,
                "machine_id": n.machine_id,
                "status": n.status,
                "last_heartbeat": n.last_heartbeat.isoformat() if n.last_heartbeat else None,
                "disks": [
                    {
                        "disk_id": d.disk_id,
                        "mount_path": d.mount_path,
                        "total_bytes": d.total_bytes,
                        "used_bytes": d.used_bytes,
                        "free_bytes": d.free_bytes,
                        "usage_pct": round(d.usage_pct, 2),
                        "is_active": d.is_active,
                    }
                    for d in sorted(n.disks, key=lambda item: item.disk_id)
                ],
            }
            for n in nodes
        ],
        "nodes_by_status": nodes_by_status,
        "storage_summary": {
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "free_bytes": free_bytes,
            "usage_pct": usage_pct,
        },
        "files_by_status": {
            st.value: entry_counts.get(st.value, 0)
            for st in EntryStatus
            if st != EntryStatus.DIRECTORY
        },
        "active_re_replications": active_replications,
    }


@router.get("/metrics", status_code=status.HTTP_200_OK)
async def get_metrics(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retorna métricas de observabilidade em JSON."""
    stmt_rep = select(BlockReplica.status, func.count()).group_by(BlockReplica.status)
    rep_counts = dict((await db.execute(stmt_rep)).all())

    stmt_files = (
        select(Entry.status, func.count(Entry.entry_id))
        .where(Entry.is_dir.is_(False))
        .group_by(Entry.status)
    )
    file_counts = dict((await db.execute(stmt_files)).all())

    stmt_nodes = select(StorageNode.status, func.count(StorageNode.node_id)).group_by(StorageNode.status)
    node_counts = dict((await db.execute(stmt_nodes)).all())

    return {
        "nodes_by_status": {st.value: node_counts.get(st.value, 0) for st in NodeStatus},
        "replicas_by_status": {st.value: rep_counts.get(st.value, 0) for st in ReplicaStatus},
        "files_by_status": {
            st.value: file_counts.get(st.value, 0)
            for st in EntryStatus
            if st != EntryStatus.DIRECTORY
        },
    }

