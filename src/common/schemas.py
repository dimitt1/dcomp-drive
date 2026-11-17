"""Schemas Pydantic compartilhados para contratos REST do DComp Drive."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class NodeStatus(str, Enum):
    """Estados possíveis de um Nó de Armazenamento."""

    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
    DRAINING = "DRAINING"


class EntryStatus(str, Enum):
    """Estados possíveis de uma entrada na árvore de metadados (RF1, RF8)."""

    DIRECTORY = "DIRECTORY"
    WRITING = "WRITING"
    HEALTHY = "HEALTHY"
    SUB_REPLICATED = "SUB_REPLICATED"
    RECOVERING = "RECOVERING"
    CORRUPTED = "CORRUPTED"


class ReplicaStatus(str, Enum):
    """Estados possíveis de uma réplica de bloco."""

    PENDING = "PENDING"
    VERIFIED = "VERIFIED"
    COPYING = "COPYING"
    CORRUPTED = "CORRUPTED"


# =====================================================================
# Schemas de Metadados, Arquivos e Diretórios
# =====================================================================


class BlockAllocateItem(BaseModel):
    """Especificação de um bloco enviado pelo cliente na alocação."""

    block_index: int = Field(..., ge=0, description="Ordem do chunk no arquivo (0, 1, 2...)")
    size_bytes: int = Field(..., ge=0, description="Tamanho do bloco em bytes")
    block_hash: str = Field(..., min_length=64, max_length=64, description="SHA-256 do chunk")
    encryption_nonce: Optional[str] = Field(
        default=None,
        description="Nonce hexadecimal do AES-256-GCM se o bloco estiver cifrado",
    )


class FileAllocateRequest(BaseModel):
    """Payload de requisição para POST /files/allocate."""

    path: str = Field(..., min_length=1, description="Caminho absoluto do arquivo (ex: /docs/relatorio.pdf)")
    size_bytes: int = Field(..., ge=0, description="Tamanho total do arquivo em bytes")
    file_hash: str = Field(..., min_length=64, max_length=64, description="SHA-256 do arquivo completo em texto claro")
    is_encrypted: bool = Field(default=False, description="Indica se os blocos estão cifrados")
    replication_target: Optional[int] = Field(
        default=None,
        ge=1,
        description="Nível N de replicação desejado (usa config padrão se omitido)",
    )
    blocks: list[BlockAllocateItem] = Field(default_factory=list, description="Lista de blocos do arquivo")


class ReplicaTarget(BaseModel):
    """Destino alocado para gravação de uma réplica de bloco."""

    node_id: str
    host_url: str
    machine_id: str
    disk_id: str


class AllocatedBlockResponse(BaseModel):
    """Bloco criado com seus respectivos destinos de gravação."""

    block_id: UUID
    block_index: int
    size_bytes: int
    block_hash: str
    encryption_nonce: Optional[str] = None
    targets: list[ReplicaTarget]


class FileAllocateResponse(BaseModel):
    """Resposta do POST /files/allocate."""

    entry_id: UUID
    path: str
    status: EntryStatus
    replication_target: int
    write_quorum: int
    lease_expires_at: datetime
    under_replicated_warning: Optional[str] = None
    blocks: list[AllocatedBlockResponse]


class ConfirmedReplicaItem(BaseModel):
    """Confirmação de réplica gravada e verificada no Storage Node."""

    block_id: UUID
    node_id: str
    disk_id: str


class FileCommitRequest(BaseModel):
    """Payload de requisição para POST /files/commit."""

    entry_id: UUID
    confirmed_replicas: list[ConfirmedReplicaItem]


class FileCommitResponse(BaseModel):
    """Resposta do POST /files/commit."""

    entry_id: UUID
    path: str
    status: EntryStatus
    replication_target: int
    write_quorum_used: int
    total_blocks: int
    fully_replicated_blocks: int
    sub_replicated_blocks: int
    message: str


class ReplicaMetadataResponse(BaseModel):
    """Metadados de uma réplica de bloco retornados em consultas."""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    host_url: str
    machine_id: str
    disk_id: str
    status: ReplicaStatus
    node_status: NodeStatus


class BlockMetadataResponse(BaseModel):
    """Metadados de um bloco e localização de suas réplicas."""

    model_config = ConfigDict(from_attributes=True)

    block_id: UUID
    block_index: int
    size_bytes: int
    block_hash: str
    encryption_nonce: Optional[str] = None
    replicas: list[ReplicaMetadataResponse]


class FileMetadataResponse(BaseModel):
    """Metadados completos de um arquivo ou diretório (GET /files/metadata)."""

    model_config = ConfigDict(from_attributes=True)

    entry_id: UUID
    path: str
    parent_path: str
    is_dir: bool
    size_bytes: int
    file_hash: Optional[str] = None
    replication_target: int
    is_encrypted: bool
    status: EntryStatus
    lease_expires_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    blocks: list[BlockMetadataResponse] = Field(default_factory=list)


class DirectoryCreateRequest(BaseModel):
    """Payload para criação de diretório (POST /dirs)."""

    path: str = Field(..., min_length=1, description="Caminho absoluto do diretório")
    parents: bool = Field(default=True, description="Cria diretórios pais automaticamente se não existirem")


class EntrySummaryResponse(BaseModel):
    """Resumo de entrada em listagem de diretório."""

    model_config = ConfigDict(from_attributes=True)

    entry_id: UUID
    path: str
    name: str
    parent_path: str
    is_dir: bool
    size_bytes: int
    file_hash: Optional[str] = None
    replication_target: int
    is_encrypted: bool
    status: EntryStatus
    created_at: datetime
    updated_at: datetime


class DirectoryListResponse(BaseModel):
    """Resposta da listagem de diretório (GET /dirs)."""

    path: str
    total_items: int
    entries: list[EntrySummaryResponse]


class EntryMoveRequest(BaseModel):
    """Payload para renomear ou mover arquivo/diretório (PATCH /entries/move)."""

    source_path: str = Field(..., min_length=1)
    destination_path: str = Field(..., min_length=1)


class EntryDeleteResponse(BaseModel):
    """Resposta da remoção de arquivo ou diretório (DELETE /entries)."""

    deleted_path: str
    deleted_entries_count: int
    scheduled_replica_deletions: int


class ReportCorruptionRequest(BaseModel):
    """Payload para reporte de corrupção de bloco (POST /blocks/report-corruption)."""

    block_id: UUID
    node_id: str
    disk_id: str
    reporter: str = Field(default="client", description="Quem detectou a corrupção (client, scrubber, storage_node)")


class ReportCorruptionResponse(BaseModel):
    """Resposta ao reporte de corrupção de réplica."""

    block_id: UUID
    node_id: str
    disk_id: str
    entry_id: UUID
    file_path: str
    file_status: EntryStatus
    healthy_replicas_remaining: int
    re_replication_triggered: bool
    message: str


# =====================================================================
# Schemas de Cluster, Heartbeat e Espaço
# =====================================================================


class DiskReportItem(BaseModel):
    """Relatório de espaço de um disco local do Storage Node."""

    disk_id: str
    mount_path: str
    total_bytes: int = Field(..., ge=0)
    used_bytes: int = Field(..., ge=0)
    is_active: bool = True


class NodeRegisterRequest(BaseModel):
    """Payload de registro ativo e reconciliação de nó (RF6)."""

    node_id: str
    host_url: str
    machine_id: str
    disks: list[DiskReportItem]
    existing_blocks: dict[str, list[UUID]] = Field(
        default_factory=dict,
        description="Mapa disk_id -> lista de block_ids já armazenados no disco para reconciliação",
    )


class NodeHeartbeatRequest(BaseModel):
    """Payload de heartbeat periódico (RF5, RF7)."""

    node_id: str
    disks: list[DiskReportItem]
