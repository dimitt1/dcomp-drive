"""Modelagem relacional SQLAlchemy 2.0 (PostgreSQL) para o Nó de Metadados (RF10)."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from src.common.schemas import EntryStatus, NodeStatus, ReplicaStatus


def utc_now() -> datetime:
    """Retorna o timestamp UTC atual com timezone explícito."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Classe base declarativa para os modelos ORM do Controlador."""


class StorageNode(Base):
    """Representa um Nó de Armazenamento registrado no cluster."""

    __tablename__ = "storage_nodes"

    node_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    host_url: Mapped[str] = mapped_column(String(256), nullable=False)
    machine_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=NodeStatus.ALIVE.value,
        index=True,
    )
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    disks: Mapped[list["NodeDisk"]] = relationship(
        "NodeDisk",
        back_populates="node",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    replicas: Mapped[list["BlockReplica"]] = relationship(
        "BlockReplica",
        back_populates="node",
        cascade="all, delete-orphan",
    )


class NodeDisk(Base):
    """Representa um disco (diretório de dados) gerenciado por um Nó de Armazenamento."""

    __tablename__ = "node_disks"

    disk_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    node_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("storage_nodes.node_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    mount_path: Mapped[str] = mapped_column(String(256), nullable=False)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    used_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    node: Mapped["StorageNode"] = relationship("StorageNode", back_populates="disks")
    replicas: Mapped[list["BlockReplica"]] = relationship(
        "BlockReplica",
        back_populates="disk",
        cascade="all, delete-orphan",
    )

    @property
    def free_bytes(self) -> int:
        return max(0, self.total_bytes - self.used_bytes)

    @property
    def usage_pct(self) -> float:
        if self.total_bytes <= 0:
            return 100.0
        return (self.used_bytes / self.total_bytes) * 100.0


class Entry(Base):
    """Representa um arquivo ou diretório na árvore de metadados do DComp Drive."""

    __tablename__ = "entries"

    entry_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    path: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False, index=True)
    parent_path: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    is_dir: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    replication_target: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=EntryStatus.WRITING.value,
        index=True,
    )
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )

    blocks: Mapped[list["Block"]] = relationship(
        "Block",
        back_populates="entry",
        cascade="all, delete-orphan",
        order_by="Block.block_index",
        lazy="selectin",
    )


class Block(Base):
    """Representa um bloco (chunk) de um arquivo e seu hash SHA-256."""

    __tablename__ = "blocks"
    __table_args__ = (
        UniqueConstraint("entry_id", "block_index", name="uq_block_entry_index"),
    )

    block_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    entry_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("entries.entry_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    block_index: Mapped[int] = mapped_column(Integer, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    block_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    encryption_nonce: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    entry: Mapped["Entry"] = relationship("Entry", back_populates="blocks")
    replicas: Mapped[list["BlockReplica"]] = relationship(
        "BlockReplica",
        back_populates="block",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class BlockReplica(Base):
    """Representa uma réplica de um bloco alocada/armazenada em um disco de um nó.

    Garante no nível de banco de dados (RF3):
    - PRIMARY KEY (block_id, disk_id): nunca duas réplicas do mesmo bloco no mesmo disco.
    - UNIQUE (block_id, node_id): nunca duas réplicas do mesmo bloco no mesmo nó.
    """

    __tablename__ = "block_replicas"
    __table_args__ = (
        PrimaryKeyConstraint("block_id", "disk_id", name="pk_block_replica_disk"),
        UniqueConstraint("block_id", "node_id", name="uq_block_replica_node"),
        Index("ix_block_replicas_status", "status"),
    )

    block_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("blocks.block_id", ondelete="CASCADE"),
        nullable=False,
    )
    node_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("storage_nodes.node_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    disk_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("node_disks.disk_id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=ReplicaStatus.PENDING.value,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )

    block: Mapped["Block"] = relationship("Block", back_populates="replicas")
    node: Mapped["StorageNode"] = relationship("StorageNode", back_populates="replicas", lazy="selectin")
    disk: Mapped["NodeDisk"] = relationship("NodeDisk", back_populates="replicas", lazy="selectin")
