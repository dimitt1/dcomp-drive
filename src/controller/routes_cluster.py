"""Rotas de Cluster, Descoberta, Heartbeat, Espaço e Observabilidade (RF5, RF6, RF7, RF11)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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

logger = get_logger("controller.cluster")

router = APIRouter(tags=["Cluster & Nodes"])


@router.post("/nodes/register", status_code=status.HTTP_200_OK)
async def register_node(
    request: NodeRegisterRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Registra ou reconcilia um Nó de Armazenamento no Controlador (RF6)."""
    now = utc_now()
    stmt = (
        select(StorageNode)
        .options(selectinload(StorageNode.disks))
        .where(StorageNode.node_id == request.node_id)
    )
    node = (await db.execute(stmt)).scalar_one_or_none()

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
        node.host_url = request.host_url
        node.machine_id = request.machine_id
        if node.status != NodeStatus.DRAINING.value:
            node.status = NodeStatus.ALIVE.value
        node.last_heartbeat = now
        existing_disks = {d.disk_id: d for d in node.disks}

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

    await db.flush()

    # Reconciliação de blocos ao retornar após falha (RF6)
    obsolete_blocks: dict[str, list[str]] = {}
    reconciled_count = 0
    for disk_id, block_ids in request.existing_blocks.items():
        for bid in block_ids:
            stmt_blk = select(Block).where(Block.block_id == bid)
            blk = (await db.execute(stmt_blk)).scalar_one_or_none()
            if blk is None:
                obsolete_blocks.setdefault(disk_id, []).append(str(bid))
                continue

            stmt_rep = select(BlockReplica).where(
                BlockReplica.block_id == bid,
                BlockReplica.node_id == request.node_id,
            )
            rep = (await db.execute(stmt_rep)).scalar_one_or_none()
            if rep is not None and rep.disk_id == disk_id:
                rep.status = ReplicaStatus.VERIFIED.value
                rep.updated_at = now
                reconciled_count += 1
            elif rep is None:
                # Aproveita réplica íntegra se o bloco ainda precisar de réplicas
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

    await db.commit()
    logger.info(
        "node_registered",
        node_id=node.node_id,
        machine_id=node.machine_id,
        disks_count=len(request.disks),
        reconciled_blocks=reconciled_count,
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

    node.last_heartbeat = now
    if node.status in (NodeStatus.SUSPECT.value, NodeStatus.DEAD.value):
        node.status = NodeStatus.ALIVE.value

    disk_map = {d.disk_id: d for d in node.disks}
    for d_req in request.disks:
        if d_req.disk_id in disk_map:
            d = disk_map[d_req.disk_id]
            d.total_bytes = d_req.total_bytes
            d.used_bytes = d_req.used_bytes
            d.is_active = d_req.is_active

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
    await db.commit()
    logger.info("node_draining_started", node_id=node_id)
    return {"node_id": node_id, "status": node.status}


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
                    for d in n.disks
                ],
            }
            for n in nodes
        ],
        "files_by_status": {st.value: entry_counts.get(st.value, 0) for st in EntryStatus if st != EntryStatus.DIRECTORY},
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

    return {
        "replicas_by_status": {st.value: rep_counts.get(st.value, 0) for st in ReplicaStatus},
        "files_by_status": file_counts,
    }
