"""Rotas de Metadados, Arquivos, Diretórios, Concorrência e Integridade (RF1, RF3, RF4, RF8, RF10)."""

from __future__ import annotations

import posixpath
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.common.config import get_settings
from src.common.logger import get_logger
from src.common.schemas import (
    AllocatedBlockResponse,
    BlockMetadataResponse,
    DirectoryCreateRequest,
    DirectoryListResponse,
    EntryDeleteResponse,
    EntryMoveRequest,
    EntryStatus,
    EntrySummaryResponse,
    FileAllocateRequest,
    FileAllocateResponse,
    FileCommitRequest,
    FileCommitResponse,
    FileMetadataResponse,
    NodeStatus,
    ReplicaMetadataResponse,
    ReplicaStatus,
    ReportCorruptionRequest,
    ReportCorruptionResponse,
)
from src.controller.allocator import (
    InsufficientStorageNodesError,
    get_eligible_disks,
    select_targets_for_block,
)
from src.controller.database import get_db, get_session_factory
from src.controller.models import Block, BlockReplica, Entry, NodeDisk, StorageNode, utc_now

logger = get_logger("controller.metadata")

router = APIRouter(tags=["Metadata & Files"])


# =====================================================================
# Utilitários de Caminho e Hierarquia de Diretórios
# =====================================================================


def normalize_path(raw_path: str) -> str:
    """Normaliza um caminho absoluto POSIX e valida contra path traversal."""
    if not raw_path or "\x00" in raw_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Caminho inválido ou vazio.",
        )

    cleaned = raw_path.strip()
    if not cleaned.startswith("/"):
        cleaned = "/" + cleaned

    parts = [p for p in cleaned.split("/") if p]
    if any(p in (".", "..") for p in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Caminho não pode conter segmentos relativos ('.' ou '..').",
        )

    normalized = posixpath.normpath(cleaned)
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized


def get_parent_path(normalized_path: str) -> str:
    """Retorna o caminho do diretório pai de um caminho normalizado."""
    if normalized_path == "/":
        return "/"
    parent = posixpath.dirname(normalized_path)
    return parent if parent else "/"


def get_entry_name(normalized_path: str) -> str:
    """Retorna o nome base do arquivo ou diretório."""
    if normalized_path == "/":
        return "/"
    return posixpath.basename(normalized_path)


async def ensure_parent_directories(
    db: AsyncSession,
    parent_path: str,
    replication_target: int,
    allow_auto_create: bool = True,
) -> None:
    """Garante que toda a cadeia de diretórios pais exista até `parent_path`."""
    if parent_path == "/":
        stmt_root = select(Entry).where(Entry.path == "/")
        root = (await db.execute(stmt_root)).scalar_one_or_none()
        if root is None:
            db.add(
                Entry(
                    path="/",
                    parent_path="/",
                    is_dir=True,
                    size_bytes=0,
                    replication_target=replication_target,
                    is_encrypted=False,
                    status=EntryStatus.DIRECTORY.value,
                )
            )
            await db.flush()
        return

    segments = [seg for seg in parent_path.split("/") if seg]
    current = ""
    for seg in segments:
        prev = current if current else "/"
        current = f"{current}/{seg}"
        stmt = select(Entry).where(Entry.path == current)
        existing = (await db.execute(stmt)).scalar_one_or_none()
        if existing is not None:
            if not existing.is_dir:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"O caminho pai '{current}' já existe e é um arquivo, não um diretório.",
                )
        else:
            if not allow_auto_create:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"O diretório pai '{current}' não existe.",
                )
            new_dir = Entry(
                path=current,
                parent_path=prev,
                is_dir=True,
                size_bytes=0,
                replication_target=replication_target,
                is_encrypted=False,
                status=EntryStatus.DIRECTORY.value,
            )
            db.add(new_dir)
            await db.flush()


# =====================================================================
# Limpeza de Blocos Físicos e Leases Expirados (RF8)
# =====================================================================


async def delete_physical_replicas_on_nodes(
    replicas_to_delete: list[tuple[UUID, str, str]],
) -> None:
    """Dispara DELETE assíncrono (best-effort) para os Storage Nodes apagarem arquivos .blk."""
    if not replicas_to_delete:
        return

    async with httpx.AsyncClient(timeout=5.0) as client:
        for block_id, disk_id, host_url in replicas_to_delete:
            url = f"{host_url.rstrip('/')}/blocks/{block_id}"
            try:
                await client.delete(url, params={"disk_id": disk_id})
                logger.info(
                    "physical_block_deleted",
                    block_id=str(block_id),
                    disk_id=disk_id,
                    host_url=host_url,
                )
            except Exception as exc:
                logger.warning(
                    "physical_block_delete_failed",
                    block_id=str(block_id),
                    disk_id=disk_id,
                    host_url=host_url,
                    error=str(exc),
                )


def _collect_physical_replicas(entry: Entry) -> list[tuple[UUID, str, str]]:
    """Extrai tuplas (block_id, disk_id, host_url) de todas as réplicas de um Entry."""
    items: list[tuple[UUID, str, str]] = []
    for block in entry.blocks:
        for rep in block.replicas:
            if rep.node and rep.node.host_url:
                items.append((block.block_id, rep.disk_id, rep.node.host_url))
    return items


def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Garante que um datetime vindo do banco tenha timezone UTC explícito."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def cleanup_expired_leases(db: AsyncSession) -> list[tuple[UUID, str, str]]:
    """Remove entradas com lease de escrita (WRITING) expirado (RF8).

    Retorna a lista de blocos físicos órfãos para remoção em background nos Storage Nodes.
    """
    now = utc_now()
    stmt = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(
            Entry.status == EntryStatus.WRITING.value,
            Entry.lease_expires_at.is_not(None),
            Entry.lease_expires_at <= now,
        )
    )
    result = await db.execute(stmt)
    expired_entries = result.scalars().all()

    orphaned_replicas: list[tuple[UUID, str, str]] = []
    for entry in expired_entries:
        orphaned_replicas.extend(_collect_physical_replicas(entry))
        logger.warning(
            "lease_expired_cleaned",
            entry_id=str(entry.entry_id),
            path=entry.path,
            lease_expires_at=str(entry.lease_expires_at),
        )
        await db.delete(entry)

    if expired_entries:
        await db.commit()

    return orphaned_replicas


def evaluate_entry_health(entry: Entry) -> EntryStatus:
    """Calcula dinamicamente o estado de integridade/replicação de um arquivo (RF1, RF5)."""
    if entry.is_dir:
        return EntryStatus.DIRECTORY
    if entry.status == EntryStatus.WRITING.value:
        return EntryStatus.WRITING

    if not entry.blocks:
        return EntryStatus(entry.status)

    has_sub_replicated = False
    has_copying = False

    for block in entry.blocks:
        alive_verified = [
            r
            for r in block.replicas
            if r.status == ReplicaStatus.VERIFIED.value
            and r.node is not None
            and r.node.status == NodeStatus.ALIVE.value
        ]
        readable_verified = [
            r
            for r in block.replicas
            if r.status == ReplicaStatus.VERIFIED.value
            and r.node is not None
            and r.node.status in (NodeStatus.ALIVE.value, NodeStatus.DRAINING.value)
        ]
        copying = [r for r in block.replicas if r.status == ReplicaStatus.COPYING.value]

        if copying:
            has_copying = True

        if len(readable_verified) == 0:
            return EntryStatus.CORRUPTED

        if len(alive_verified) < entry.replication_target:
            has_sub_replicated = True

    if has_sub_replicated:
        if has_copying or entry.status == EntryStatus.RECOVERING.value:
            return EntryStatus.RECOVERING
        return EntryStatus.SUB_REPLICATED

    return EntryStatus.HEALTHY


def build_file_metadata_response(
    entry: Entry,
    include_all_replicas: bool = False,
) -> FileMetadataResponse:
    """Constrói o FileMetadataResponse filtrando réplicas legíveis por padrão."""
    blocks_resp: list[BlockMetadataResponse] = []
    if not entry.is_dir:
        sorted_blocks = sorted(entry.blocks, key=lambda b: b.block_index)
        for block in sorted_blocks:
            replicas_resp: list[ReplicaMetadataResponse] = []
            for rep in block.replicas:
                if rep.node is None:
                    continue
                node_st = NodeStatus(rep.node.status)
                rep_st = ReplicaStatus(rep.status)

                if not include_all_replicas:
                    # Por padrão (para leitura do cliente), retorna apenas réplicas VERIFIED em nós ALIVE/DRAINING
                    if entry.status == EntryStatus.WRITING.value:
                        continue
                    if rep_st != ReplicaStatus.VERIFIED:
                        continue
                    if node_st not in (NodeStatus.ALIVE, NodeStatus.DRAINING):
                        continue

                replicas_resp.append(
                    ReplicaMetadataResponse(
                        node_id=rep.node_id,
                        host_url=rep.node.host_url,
                        machine_id=rep.node.machine_id,
                        disk_id=rep.disk_id,
                        status=rep_st,
                        node_status=node_st,
                    )
                )

            # Prioriza nós ALIVE antes de DRAINING
            replicas_resp.sort(key=lambda r: (0 if r.node_status == NodeStatus.ALIVE else 1, r.node_id))

            blocks_resp.append(
                BlockMetadataResponse(
                    block_id=block.block_id,
                    block_index=block.block_index,
                    size_bytes=block.size_bytes,
                    block_hash=block.block_hash,
                    encryption_nonce=block.encryption_nonce,
                    replicas=replicas_resp,
                )
            )

    return FileMetadataResponse(
        entry_id=entry.entry_id,
        path=entry.path,
        parent_path=entry.parent_path,
        is_dir=entry.is_dir,
        size_bytes=entry.size_bytes,
        file_hash=entry.file_hash,
        replication_target=entry.replication_target,
        is_encrypted=entry.is_encrypted,
        status=EntryStatus(entry.status),
        lease_expires_at=_ensure_aware(entry.lease_expires_at),
        created_at=_ensure_aware(entry.created_at) or utc_now(),
        updated_at=_ensure_aware(entry.updated_at) or utc_now(),
        blocks=blocks_resp,
    )


# =====================================================================
# Endpoints de Alocação, Commit e Consulta de Arquivos
# =====================================================================


@router.post(
    "/files/allocate",
    response_model=FileAllocateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Aloca destinos para gravação de um arquivo e cria lease de escrita (RF1, RF3, RF8)",
)
async def allocate_file(
    request: FileAllocateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> FileAllocateResponse:
    settings = get_settings()

    # Limpa leases expirados antes de verificar conflitos
    orphans = await cleanup_expired_leases(db)
    if orphans:
        background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)

    path = normalize_path(request.path)
    if path == "/":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não é possível alocar um arquivo no caminho raiz '/'.",
        )

    parent_path = get_parent_path(path)
    target_n = request.replication_target or settings.replication_factor

    # Valida a lista de blocos e seus tamanhos
    sorted_req_blocks = sorted(request.blocks, key=lambda b: b.block_index)
    for expected_idx, blk in enumerate(sorted_req_blocks):
        if blk.block_index != expected_idx:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Índices de blocos devem ser contíguos a partir de 0 (esperado {expected_idx}, recebido {blk.block_index}).",
            )

    # size_bytes do arquivo representa texto claro; blocos representam bytes de rede.
    # AESGCM.encrypt acrescenta uma tag de autenticação de 16 bytes por bloco.
    for blk in sorted_req_blocks:
        if request.is_encrypted:
            try:
                nonce = bytes.fromhex(blk.encryption_nonce or "")
            except ValueError:
                nonce = b""
            if len(nonce) != 12 or len(blk.encryption_nonce or "") != 24 or blk.size_bytes < 16:
                raise HTTPException(status_code=400, detail="Bloco AES-GCM exige nonce hexadecimal de 12 bytes e tag de 16 bytes.")
        elif blk.encryption_nonce is not None:
            raise HTTPException(status_code=400, detail="Bloco não cifrado não deve conter nonce.")
    total_blocks_bytes = sum(b.size_bytes - (16 if request.is_encrypted else 0) for b in sorted_req_blocks)
    if total_blocks_bytes != request.size_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Soma dos tamanhos dos blocos ({total_blocks_bytes}) difere de size_bytes ({request.size_bytes}).",
        )

    # Garante que os diretórios pais existam
    await ensure_parent_directories(db, parent_path, target_n, allow_auto_create=True)

    # Verifica se já existe entrada no mesmo path (Controle de Concorrência RF8)
    stmt_existing = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path == path)
    )
    existing_entry = (await db.execute(stmt_existing)).scalar_one_or_none()

    old_replicas_to_delete: list[tuple[UUID, str, str]] = []
    now = utc_now()

    if existing_entry is not None:
        if existing_entry.is_dir:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Já existe um diretório no caminho '{path}'.",
            )
        lease_exp = _ensure_aware(existing_entry.lease_expires_at)
        if (
            existing_entry.status == EntryStatus.WRITING.value
            and lease_exp is not None
            and lease_exp > now
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Conflito de concorrência (RF8): o arquivo '{path}' está sendo gravado "
                    f"por outro cliente (lease ativo até {lease_exp.isoformat()})."
                ),
            )
        # Sobrescrita de arquivo já finalizado: agenda remoção dos blocos antigos
        old_replicas_to_delete = _collect_physical_replicas(existing_entry)
        await db.execute(delete(Block).where(Block.entry_id == existing_entry.entry_id))
        await db.flush()

    # Seleciona discos candidatos e aloca N destinos para cada bloco
    candidates = await get_eligible_disks(db, settings)
    allocated_blocks_plan: list[tuple[ int, int, str, Optional[str], list ]] = []
    under_replicated_warning: Optional[str] = None
    min_targets_count = target_n

    try:
        for blk_req in sorted_req_blocks:
            targets, warning = select_targets_for_block(
                candidates=candidates,
                block_size_bytes=blk_req.size_bytes,
                replication_target=target_n,
                settings=settings,
            )
            if warning and under_replicated_warning is None:
                under_replicated_warning = warning
            min_targets_count = min(min_targets_count, len(targets))
            allocated_blocks_plan.append(
                (
                    blk_req.block_index,
                    blk_req.size_bytes,
                    blk_req.block_hash,
                    blk_req.encryption_nonce,
                    targets,
                )
            )
    except InsufficientStorageNodesError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    lease_expires = now + timedelta(seconds=settings.lease_timeout_sec)

    if existing_entry is not None:
        entry = existing_entry
        entry.parent_path = parent_path
        entry.size_bytes = request.size_bytes
        entry.file_hash = request.file_hash
        entry.replication_target = target_n
        entry.is_encrypted = request.is_encrypted
        entry.status = EntryStatus.WRITING.value
        entry.lease_expires_at = lease_expires
        entry.updated_at = now
    else:
        entry = Entry(
            path=path,
            parent_path=parent_path,
            is_dir=False,
            size_bytes=request.size_bytes,
            file_hash=request.file_hash,
            replication_target=target_n,
            is_encrypted=request.is_encrypted,
            status=EntryStatus.WRITING.value,
            lease_expires_at=lease_expires,
            created_at=now,
            updated_at=now,
        )
        db.add(entry)
        await db.flush()

    allocated_blocks_resp: list[AllocatedBlockResponse] = []
    for block_index, size_bytes, block_hash, encryption_nonce, targets in allocated_blocks_plan:
        new_block = Block(
            entry_id=entry.entry_id,
            block_index=block_index,
            size_bytes=size_bytes,
            block_hash=block_hash,
            encryption_nonce=encryption_nonce,
        )
        db.add(new_block)
        await db.flush()

        for t in targets:
            replica = BlockReplica(
                block_id=new_block.block_id,
                node_id=t.node_id,
                disk_id=t.disk_id,
                status=ReplicaStatus.PENDING.value,
            )
            db.add(replica)

        allocated_blocks_resp.append(
            AllocatedBlockResponse(
                block_id=new_block.block_id,
                block_index=block_index,
                size_bytes=size_bytes,
                block_hash=block_hash,
                encryption_nonce=encryption_nonce,
                targets=targets,
            )
        )

    await db.commit()

    if old_replicas_to_delete:
        background_tasks.add_task(delete_physical_replicas_on_nodes, old_replicas_to_delete)

    effective_quorum = (
        min(settings.write_quorum, target_n, min_targets_count)
        if sorted_req_blocks
        else min(settings.write_quorum, target_n)
    )

    logger.info(
        "file_allocated",
        entry_id=str(entry.entry_id),
        path=entry.path,
        size_bytes=entry.size_bytes,
        blocks_count=len(allocated_blocks_resp),
        replication_target=target_n,
        write_quorum=effective_quorum,
        under_replicated=under_replicated_warning is not None,
    )

    return FileAllocateResponse(
        entry_id=entry.entry_id,
        path=entry.path,
        status=EntryStatus.WRITING,
        replication_target=target_n,
        write_quorum=effective_quorum,
        lease_expires_at=lease_expires,
        under_replicated_warning=under_replicated_warning,
        blocks=allocated_blocks_resp,
    )


@router.post(
    "/files/commit",
    response_model=FileCommitResponse,
    summary="Confirma a gravação dos blocos após atingir o quórum de escrita W (RF3, RF8)",
)
async def commit_file(
    request: FileCommitRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> FileCommitResponse:
    settings = get_settings()
    now = utc_now()

    stmt = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.entry_id == request.entry_id)
    )
    entry = (await db.execute(stmt)).scalar_one_or_none()

    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Arquivo com entry_id '{request.entry_id}' não encontrado.",
        )

    if entry.is_dir:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não é possível realizar commit em um diretório.",
        )

    if entry.status != EntryStatus.WRITING.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"O arquivo '{entry.path}' não está em estado WRITING (estado atual: {entry.status}).",
        )

    lease_exp = _ensure_aware(entry.lease_expires_at)
    if lease_exp is not None and lease_exp <= now:
        orphans = _collect_physical_replicas(entry)
        await db.delete(entry)
        await db.commit()
        if orphans:
            background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Lease de escrita para '{entry.path}' expirou antes do commit.",
        )

    # Mapeia confirmações enviadas pelo cliente: block_id -> set((node_id, disk_id))
    confirmed_map: dict[UUID, set[tuple[str, str]]] = {}
    for item in request.confirmed_replicas:
        confirmed_map.setdefault(item.block_id, set()).add((item.node_id, item.disk_id))

    fully_replicated_blocks = 0
    sub_replicated_blocks = 0
    unconfirmed_to_cleanup: list[tuple[UUID, str, str]] = []
    quorum_used = min(settings.write_quorum, entry.replication_target)

    for block in entry.blocks:
        allocated_count = len(block.replicas)
        required_quorum = min(settings.write_quorum, entry.replication_target, allocated_count)
        quorum_used = min(quorum_used, required_quorum)

        confirmed_pairs = confirmed_map.get(block.block_id, set())
        verified_replicas: list[BlockReplica] = []
        unconfirmed_replicas: list[BlockReplica] = []

        for rep in block.replicas:
            if (rep.node_id, rep.disk_id) in confirmed_pairs:
                rep.status = ReplicaStatus.VERIFIED.value
                rep.updated_at = now
                verified_replicas.append(rep)
            else:
                unconfirmed_replicas.append(rep)

        if len(verified_replicas) < required_quorum:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Quórum de escrita não atingido para o bloco {block.block_index} "
                    f"({block.block_id}): confirmadas={len(verified_replicas)}, "
                    f"exigidas={required_quorum}."
                ),
            )

        # Remove réplicas PENDING não confirmadas para que o monitor re-replique em background
        for unconf in unconfirmed_replicas:
            if unconf.node and unconf.node.host_url:
                unconfirmed_to_cleanup.append((block.block_id, unconf.disk_id, unconf.node.host_url))
            await db.delete(unconf)

        if len(verified_replicas) >= entry.replication_target:
            fully_replicated_blocks += 1
        else:
            sub_replicated_blocks += 1

    if sub_replicated_blocks > 0:
        entry.status = EntryStatus.SUB_REPLICATED.value
        msg = (
            f"Arquivo confirmado com quórum W={quorum_used} em estado SUB_REPLICATED "
            f"({sub_replicated_blocks} bloco(s) aguardando re-replicação em segundo plano)."
        )
    else:
        entry.status = EntryStatus.HEALTHY.value
        msg = f"Arquivo confirmado e íntegro (HEALTHY) com N={entry.replication_target} réplicas por bloco."

    entry.lease_expires_at = None
    entry.updated_at = now
    await db.commit()

    if unconfirmed_to_cleanup:
        background_tasks.add_task(delete_physical_replicas_on_nodes, unconfirmed_to_cleanup)

    logger.info(
        "file_committed",
        entry_id=str(entry.entry_id),
        path=entry.path,
        status=entry.status,
        fully_replicated_blocks=fully_replicated_blocks,
        sub_replicated_blocks=sub_replicated_blocks,
    )

    return FileCommitResponse(
        entry_id=entry.entry_id,
        path=entry.path,
        status=EntryStatus(entry.status),
        replication_target=entry.replication_target,
        write_quorum_used=quorum_used,
        total_blocks=len(entry.blocks),
        fully_replicated_blocks=fully_replicated_blocks,
        sub_replicated_blocks=sub_replicated_blocks,
        message=msg,
    )


@router.get(
    "/files/metadata",
    response_model=FileMetadataResponse,
    summary="Consulta metadados completos do arquivo, estado e localização das réplicas (RF1)",
)
async def get_file_metadata(
    background_tasks: BackgroundTasks,
    path: str = Query(..., description="Caminho absoluto do arquivo ou diretório"),
    for_read: bool = Query(
        default=False,
        description="Se true, bloqueia leitura caso o arquivo ainda esteja em estado WRITING (RF8)",
    ),
    include_all_replicas: bool = Query(
        default=False,
        description="Se true, inclui também réplicas PENDING/CORRUPTED/DEAD para inspeção (stat)",
    ),
    db: AsyncSession = Depends(get_db),
) -> FileMetadataResponse:
    orphans = await cleanup_expired_leases(db)
    if orphans:
        background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)

    norm_path = normalize_path(path)
    stmt = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path == norm_path)
    )
    entry = (await db.execute(stmt)).scalar_one_or_none()
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Caminho '{norm_path}' não encontrado.",
        )

    if for_read and entry.status == EntryStatus.WRITING.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"O arquivo '{norm_path}' está em escrita ativa (WRITING). Leitura parcial não permitida (RF8).",
        )

    # Atualiza estado dinâmico do arquivo conforme saúde atual dos nós e réplicas
    if not entry.is_dir and entry.status != EntryStatus.WRITING.value:
        computed_status = evaluate_entry_health(entry)
        if computed_status.value != entry.status:
            entry.status = computed_status.value
            entry.updated_at = utc_now()
            await db.commit()

    return build_file_metadata_response(entry, include_all_replicas=include_all_replicas)


# =====================================================================
# Endpoints de Diretórios e Operações de Entrada (mkdir, ls, rm, mv)
# =====================================================================


@router.post(
    "/dirs",
    response_model=FileMetadataResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cria um novo diretório na árvore de metadados (RF1)",
)
async def create_directory(
    request: DirectoryCreateRequest,
    db: AsyncSession = Depends(get_db),
) -> FileMetadataResponse:
    settings = get_settings()
    norm_path = normalize_path(request.path)

    stmt_existing = select(Entry).where(Entry.path == norm_path)
    existing = (await db.execute(stmt_existing)).scalar_one_or_none()
    if existing is not None:
        if existing.is_dir:
            return build_file_metadata_response(existing)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe um arquivo no caminho '{norm_path}'.",
        )

    parent_path = get_parent_path(norm_path)
    await ensure_parent_directories(
        db,
        parent_path=parent_path,
        replication_target=settings.replication_factor,
        allow_auto_create=request.parents,
    )

    now = utc_now()
    new_dir = Entry(
        path=norm_path,
        parent_path=parent_path,
        is_dir=True,
        size_bytes=0,
        replication_target=settings.replication_factor,
        is_encrypted=False,
        status=EntryStatus.DIRECTORY.value,
        created_at=now,
        updated_at=now,
    )
    db.add(new_dir)
    await db.commit()
    await db.refresh(new_dir)

    logger.info("directory_created", entry_id=str(new_dir.entry_id), path=new_dir.path)
    return build_file_metadata_response(new_dir)


@router.get(
    "/dirs",
    response_model=DirectoryListResponse,
    summary="Lista o conteúdo de um diretório (RF1)",
)
async def list_directory(
    background_tasks: BackgroundTasks,
    path: str = Query(default="/", description="Caminho absoluto do diretório"),
    db: AsyncSession = Depends(get_db),
) -> DirectoryListResponse:
    orphans = await cleanup_expired_leases(db)
    if orphans:
        background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)

    norm_path = normalize_path(path)
    stmt_dir = select(Entry).where(Entry.path == norm_path)
    dir_entry = (await db.execute(stmt_dir)).scalar_one_or_none()

    if dir_entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Diretório '{norm_path}' não encontrado.",
        )

    if not dir_entry.is_dir:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"O caminho '{norm_path}' é um arquivo, não um diretório.",
        )

    stmt_children = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(
            Entry.parent_path == norm_path,
            Entry.path != "/",
        )
        .order_by(Entry.is_dir.desc(), Entry.path.asc())
    )
    children = (await db.execute(stmt_children)).scalars().all()

    items: list[EntrySummaryResponse] = []
    status_changed = False
    for child in children:
        if not child.is_dir and child.status != EntryStatus.WRITING.value:
            computed = evaluate_entry_health(child)
            if computed.value != child.status:
                child.status = computed.value
                status_changed = True

        items.append(
            EntrySummaryResponse(
                entry_id=child.entry_id,
                path=child.path,
                name=get_entry_name(child.path),
                parent_path=child.parent_path,
                is_dir=child.is_dir,
                size_bytes=child.size_bytes,
                file_hash=child.file_hash,
                replication_target=child.replication_target,
                is_encrypted=child.is_encrypted,
                status=EntryStatus(child.status),
                created_at=_ensure_aware(child.created_at) or utc_now(),
                updated_at=_ensure_aware(child.updated_at) or utc_now(),
            )
        )

    if status_changed:
        await db.commit()

    return DirectoryListResponse(
        path=norm_path,
        total_items=len(items),
        entries=items,
    )


@router.delete(
    "/entries",
    response_model=EntryDeleteResponse,
    summary="Remove um arquivo ou diretório e agenda exclusão dos blocos físicos (RF1)",
)
async def delete_entry(
    background_tasks: BackgroundTasks,
    path: str = Query(..., description="Caminho absoluto do arquivo ou diretório a remover"),
    recursive: bool = Query(default=True, description="Se true, remove diretórios não vazios recursivamente"),
    db: AsyncSession = Depends(get_db),
) -> EntryDeleteResponse:
    orphans = await cleanup_expired_leases(db)
    if orphans:
        background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)

    norm_path = normalize_path(path)
    if norm_path == "/":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não é permitido remover o diretório raiz '/'.",
        )

    stmt_target = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path == norm_path)
    )
    target = (await db.execute(stmt_target)).scalar_one_or_none()
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Caminho '{norm_path}' não encontrado.",
        )

    now = utc_now()
    if target.is_dir:
        prefix = f"{norm_path}/%"
        stmt_descendants = (
            select(Entry)
            .options(
                selectinload(Entry.blocks)
                .selectinload(Block.replicas)
                .selectinload(BlockReplica.node)
            )
            .where(or_(Entry.path == norm_path, Entry.path.like(prefix)))
        )
        entries_to_delete = list((await db.execute(stmt_descendants)).scalars().all())
        if len(entries_to_delete) > 1 and not recursive:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"O diretório '{norm_path}' não está vazio (use recursive=true para remover).",
            )
    else:
        entries_to_delete = [target]

    # Verifica se algum arquivo sendo removido possui lease de escrita ativo
    for ent in entries_to_delete:
        lease_exp = _ensure_aware(ent.lease_expires_at)
        if ent.status == EntryStatus.WRITING.value and lease_exp is not None and lease_exp > now:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Não é possível remover '{ent.path}' pois há uma escrita em andamento (lease ativo).",
            )

    physical_replicas: list[tuple[UUID, str, str]] = []
    for ent in entries_to_delete:
        physical_replicas.extend(_collect_physical_replicas(ent))
        await db.delete(ent)

    await db.commit()

    if physical_replicas:
        background_tasks.add_task(delete_physical_replicas_on_nodes, physical_replicas)

    logger.info(
        "entry_deleted",
        path=norm_path,
        deleted_entries_count=len(entries_to_delete),
        scheduled_replica_deletions=len(physical_replicas),
    )

    return EntryDeleteResponse(
        deleted_path=norm_path,
        deleted_entries_count=len(entries_to_delete),
        scheduled_replica_deletions=len(physical_replicas),
    )


@router.patch(
    "/entries/move",
    response_model=FileMetadataResponse,
    summary="Renomeia ou move um arquivo ou diretório (RF1)",
)
async def move_entry(
    request: EntryMoveRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> FileMetadataResponse:
    settings = get_settings()
    orphans = await cleanup_expired_leases(db)
    if orphans:
        background_tasks.add_task(delete_physical_replicas_on_nodes, orphans)

    src_path = normalize_path(request.source_path)
    dst_path = normalize_path(request.destination_path)

    if src_path == "/" or dst_path == "/":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não é permitido mover ou sobrescrever o diretório raiz '/'.",
        )

    if src_path == dst_path:
        stmt_same = (
            select(Entry)
            .options(
                selectinload(Entry.blocks)
                .selectinload(Block.replicas)
                .selectinload(BlockReplica.node)
            )
            .where(Entry.path == src_path)
        )
        same_entry = (await db.execute(stmt_same)).scalar_one_or_none()
        if same_entry is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Origem '{src_path}' não encontrada.")
        return build_file_metadata_response(same_entry)

    if dst_path.startswith(f"{src_path}/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Não é possível mover '{src_path}' para dentro de si mesmo ('{dst_path}').",
        )

    stmt_src = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path == src_path)
    )
    src_entry = (await db.execute(stmt_src)).scalar_one_or_none()
    if src_entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Caminho de origem '{src_path}' não encontrado.",
        )

    now = utc_now()
    src_lease = _ensure_aware(src_entry.lease_expires_at)
    if src_entry.status == EntryStatus.WRITING.value and src_lease is not None and src_lease > now:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"O arquivo '{src_path}' está em escrita ativa (WRITING) e não pode ser movido.",
        )

    stmt_dst = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path == dst_path)
    )
    dst_entry = (await db.execute(stmt_dst)).scalar_one_or_none()

    # Se o destino for um diretório existente e a origem for um arquivo, ou se já existir conflito:
    old_dst_replicas: list[tuple[UUID, str, str]] = []
    if dst_entry is not None:
        if dst_entry.is_dir:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"O destino '{dst_path}' já existe e é um diretório.",
            )
        if src_entry.is_dir:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Não é possível mover o diretório '{src_path}' sobre o arquivo '{dst_path}'.",
            )
        dst_lease = _ensure_aware(dst_entry.lease_expires_at)
        if dst_entry.status == EntryStatus.WRITING.value and dst_lease is not None and dst_lease > now:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"O arquivo de destino '{dst_path}' possui escrita ativa (WRITING).",
            )
        old_dst_replicas = _collect_physical_replicas(dst_entry)
        await db.delete(dst_entry)
        await db.flush()

    dst_parent = get_parent_path(dst_path)
    await ensure_parent_directories(
        db,
        parent_path=dst_parent,
        replication_target=settings.replication_factor,
        allow_auto_create=True,
    )

    if src_entry.is_dir:
        prefix = f"{src_path}/%"
        stmt_children = select(Entry).where(Entry.path.like(prefix))
        descendants = (await db.execute(stmt_children)).scalars().all()
        for child in descendants:
            suffix = child.path[len(src_path) :]
            new_child_path = f"{dst_path}{suffix}"
            child.path = new_child_path
            child.parent_path = get_parent_path(new_child_path)
            child.updated_at = now

    src_entry.path = dst_path
    src_entry.parent_path = dst_parent
    src_entry.updated_at = now

    await db.commit()

    if old_dst_replicas:
        background_tasks.add_task(delete_physical_replicas_on_nodes, old_dst_replicas)

    logger.info(
        "entry_moved",
        entry_id=str(src_entry.entry_id),
        source_path=src_path,
        destination_path=dst_path,
    )

    return build_file_metadata_response(src_entry)


# =====================================================================
# Reporte de Corrupção de Bloco e Re-replicação Imediata (RF4)
# =====================================================================


async def _replicate_corrupted_block_background(
    block_id: UUID,
    source_node_url: str,
    source_disk_id: str,
    target_node_id: str,
    target_node_url: str,
    target_disk_id: str,
    expected_hash: str,
) -> None:
    """Executa a cópia nó-a-nó em segundo plano para substituir uma réplica corrompida."""
    replicate_url = f"{source_node_url.rstrip('/')}/blocks/replicate"
    payload = {
        "block_id": str(block_id),
        "source_disk_id": source_disk_id,
        "target_node_url": target_node_url,
        "target_disk_id": target_disk_id,
        "expected_hash": expected_hash,
    }
    success = False
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(replicate_url, json=payload)
            if resp.status_code in (200, 201):
                success = True
            else:
                logger.warning(
                    "corruption_recovery_http_error",
                    block_id=str(block_id),
                    status_code=resp.status_code,
                    response=resp.text,
                )
    except Exception as exc:
        logger.warning(
            "corruption_recovery_exception",
            block_id=str(block_id),
            target_node_id=target_node_id,
            error=str(exc),
        )

    session_factory = get_session_factory()
    async with session_factory() as session:
        stmt_rep = select(BlockReplica).where(
            BlockReplica.block_id == block_id,
            BlockReplica.disk_id == target_disk_id,
        )
        rep = (await session.execute(stmt_rep)).scalar_one_or_none()
        if rep is not None:
            if success:
                rep.status = ReplicaStatus.VERIFIED.value
                rep.updated_at = utc_now()
                logger.info(
                    "corruption_recovery_completed",
                    block_id=str(block_id),
                    target_node_id=target_node_id,
                    target_disk_id=target_disk_id,
                )
            else:
                await session.delete(rep)

        stmt_blk = (
            select(Block)
            .options(selectinload(Block.entry).selectinload(Entry.blocks).selectinload(Block.replicas).selectinload(BlockReplica.node))
            .where(Block.block_id == block_id)
        )
        blk = (await session.execute(stmt_blk)).scalar_one_or_none()
        if blk and blk.entry:
            blk.entry.status = evaluate_entry_health(blk.entry).value
            blk.entry.updated_at = utc_now()

        await session.commit()


@router.post(
    "/blocks/report-corruption",
    response_model=ReportCorruptionResponse,
    summary="Registra corrupção de bloco, descarta réplica afetada e dispara recuperação (RF4)",
)
async def report_block_corruption(
    request: ReportCorruptionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> ReportCorruptionResponse:
    settings = get_settings()
    now = utc_now()

    stmt_block = (
        select(Block)
        .options(
            selectinload(Block.replicas).selectinload(BlockReplica.node),
            selectinload(Block.entry)
            .selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node),
        )
        .where(Block.block_id == request.block_id)
    )
    block = (await db.execute(stmt_block)).scalar_one_or_none()
    if block is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bloco '{request.block_id}' não encontrado nos metadados.",
        )

    target_replica: Optional[BlockReplica] = None
    for rep in block.replicas:
        if rep.disk_id == request.disk_id or rep.node_id == request.node_id:
            target_replica = rep
            break

    if target_replica is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Réplica do bloco '{request.block_id}' no nó '{request.node_id}' "
                f"(disco '{request.disk_id}') não encontrada."
            ),
        )

    corrupted_host_url = target_replica.node.host_url if target_replica.node else None
    corrupted_disk_id = target_replica.disk_id

    # Marca a réplica como CORRUPTED (RF4) e agenda remoção física do bloco corrompido
    target_replica.status = ReplicaStatus.CORRUPTED.value
    target_replica.updated_at = now

    if corrupted_host_url:
        background_tasks.add_task(
            delete_physical_replicas_on_nodes,
            [(block.block_id, corrupted_disk_id, corrupted_host_url)],
        )

    # Identifica réplicas íntegras restantes em nós vivos
    healthy_replicas = [
        r
        for r in block.replicas
        if r.disk_id != corrupted_disk_id
        and r.status == ReplicaStatus.VERIFIED.value
        and r.node is not None
        and r.node.status == NodeStatus.ALIVE.value
    ]

    entry = block.entry
    re_replication_triggered = False

    if not healthy_replicas:
        entry.status = EntryStatus.CORRUPTED.value
        entry.updated_at = now
        await db.commit()
        msg = (
            f"Réplica marcada como CORRUPTED, mas não restam réplicas íntegras em nós vivos "
            f"para recuperar o bloco {block.block_id}."
        )
        logger.error(
            "block_all_replicas_corrupted",
            entry_id=str(entry.entry_id),
            path=entry.path,
            block_id=str(block.block_id),
            reporter=request.reporter,
        )
        return ReportCorruptionResponse(
            block_id=block.block_id,
            node_id=request.node_id,
            disk_id=corrupted_disk_id,
            entry_id=entry.entry_id,
            file_path=entry.path,
            file_status=EntryStatus.CORRUPTED,
            healthy_replicas_remaining=0,
            re_replication_triggered=False,
            message=msg,
        )

    # Remove o registro da réplica corrompida do banco para liberar constraints caso necessário
    # e tenta alocar imediatamente um novo destino respeitando a regra de não repetição
    await db.delete(target_replica)
    await db.flush()

    excluded_nodes = {r.node_id for r in block.replicas if r.disk_id != corrupted_disk_id}
    excluded_nodes.add(request.node_id)
    excluded_machines = {
        r.node.machine_id
        for r in block.replicas
        if r.disk_id != corrupted_disk_id and r.node is not None
    }
    excluded_disks = {r.disk_id for r in block.replicas if r.disk_id != corrupted_disk_id}
    excluded_disks.add(corrupted_disk_id)

    candidates = await get_eligible_disks(db, settings)
    try:
        replacement_targets, _ = select_targets_for_block(
            candidates=candidates,
            block_size_bytes=block.size_bytes,
            replication_target=1,
            settings=settings,
            excluded_node_ids=excluded_nodes,
            excluded_machine_ids=excluded_machines,
            excluded_disk_ids=excluded_disks,
        )
    except InsufficientStorageNodesError:
        replacement_targets = []

    if replacement_targets:
        new_target = replacement_targets[0]
        source_rep = healthy_replicas[0]
        copying_replica = BlockReplica(
            block_id=block.block_id,
            node_id=new_target.node_id,
            disk_id=new_target.disk_id,
            status=ReplicaStatus.COPYING.value,
        )
        db.add(copying_replica)
        entry.status = EntryStatus.RECOVERING.value
        re_replication_triggered = True

        background_tasks.add_task(
            _replicate_corrupted_block_background,
            block.block_id,
            source_rep.node.host_url,
            source_rep.disk_id,
            new_target.node_id,
            new_target.host_url,
            new_target.disk_id,
            block.block_hash,
        )
        msg = (
            f"Réplica corrompida descartada. Re-replicação iniciada a partir de "
            f"'{source_rep.node_id}' para '{new_target.node_id}' ({new_target.disk_id})."
        )
    else:
        entry.status = EntryStatus.SUB_REPLICATED.value
        msg = (
            "Réplica corrompida descartada. Nenhum nó substituto imediato disponível; "
            "o monitor em segundo plano fará a re-replicação assim que houver espaço/nós."
        )

    entry.updated_at = now
    await db.commit()

    logger.warning(
        "block_corruption_reported",
        entry_id=str(entry.entry_id),
        path=entry.path,
        block_id=str(block.block_id),
        corrupted_node_id=request.node_id,
        corrupted_disk_id=corrupted_disk_id,
        reporter=request.reporter,
        re_replication_triggered=re_replication_triggered,
    )

    return ReportCorruptionResponse(
        block_id=block.block_id,
        node_id=request.node_id,
        disk_id=corrupted_disk_id,
        entry_id=entry.entry_id,
        file_path=entry.path,
        file_status=EntryStatus(entry.status),
        healthy_replicas_remaining=len(healthy_replicas),
        re_replication_triggered=re_replication_triggered,
        message=msg,
    )
