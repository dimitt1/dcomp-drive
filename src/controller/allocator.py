"""Algoritmo de posicionamento de réplicas de blocos (RF3, RF7).

Garante:
- Regra de não repetição: nunca duas réplicas do mesmo bloco no mesmo disco, nó ou máquina.
- Gerenciamento de espaço: discos com uso >= disk_usage_limit_pct ou sem espaço livre suficiente
  não recebem novas réplicas.
- Tratamento de cluster com menos máquinas vivas que N conforme allow_under_replicated_writes.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.common.config import Settings, get_settings
from src.common.schemas import NodeStatus, ReplicaTarget
from src.controller.models import NodeDisk, StorageNode


class InsufficientStorageNodesError(Exception):
    """Exceção lançada quando não há máquinas/discos suficientes para alocar um bloco."""


@dataclass
class CandidateDisk:
    """Representa um disco candidato para alocação com controle de espaço projetado."""

    node_id: str
    host_url: str
    machine_id: str
    disk_id: str
    total_bytes: int
    projected_used_bytes: int

    @property
    def free_bytes(self) -> int:
        return max(0, self.total_bytes - self.projected_used_bytes)

    @property
    def usage_pct(self) -> float:
        if self.total_bytes <= 0:
            return 100.0
        return (self.projected_used_bytes / self.total_bytes) * 100.0


async def get_eligible_disks(
    db: AsyncSession,
    settings: Settings | None = None,
) -> list[CandidateDisk]:
    """Busca todos os discos ativos pertencentes a nós ALIVE."""
    cfg = settings or get_settings()
    stmt = (
        select(NodeDisk)
        .join(StorageNode, NodeDisk.node_id == StorageNode.node_id)
        .options(selectinload(NodeDisk.node))
        .where(
            StorageNode.status == NodeStatus.ALIVE.value,
            NodeDisk.is_active.is_(True),
        )
    )
    result = await db.execute(stmt)
    disks = result.scalars().all()

    candidates: list[CandidateDisk] = []
    for d in disks:
        cand = CandidateDisk(
            node_id=d.node_id,
            host_url=d.node.host_url,
            machine_id=d.node.machine_id,
            disk_id=d.disk_id,
            total_bytes=d.total_bytes,
            projected_used_bytes=d.used_bytes,
        )
        if cand.usage_pct < cfg.disk_usage_limit_pct:
            candidates.append(cand)

    return candidates


def select_targets_for_block(
    candidates: list[CandidateDisk],
    block_size_bytes: int,
    replication_target: int,
    settings: Settings | None = None,
    excluded_node_ids: set[str] | None = None,
    excluded_machine_ids: set[str] | None = None,
    excluded_disk_ids: set[str] | None = None,
) -> tuple[list[ReplicaTarget], str | None]:
    """Seleciona até `replication_target` destinos distintos para um bloco.

    Respeita:
    - Não repetir `disk_id`, `node_id` nem `machine_id`.
    - Espaço livre >= `block_size_bytes` e uso projetado < `disk_usage_limit_pct`.
    """
    cfg = settings or get_settings()
    excl_nodes = set(excluded_node_ids or set())
    excl_machines = set(excluded_machine_ids or set())
    excl_disks = set(excluded_disk_ids or set())

    # Filtra candidatos que comportam o bloco e ainda estão abaixo do limite de uso
    valid = [
        c
        for c in candidates
        if c.disk_id not in excl_disks
        and c.node_id not in excl_nodes
        and c.machine_id not in excl_machines
        and c.free_bytes >= block_size_bytes
        and c.usage_pct < cfg.disk_usage_limit_pct
    ]

    # Agrupa por machine_id, escolhendo o melhor disco de cada máquina (maior espaço livre)
    best_per_machine: dict[str, CandidateDisk] = {}
    for c in valid:
        current = best_per_machine.get(c.machine_id)
        if current is None or c.free_bytes > current.free_bytes:
            best_per_machine[c.machine_id] = c

    machine_choices = list(best_per_machine.values())

    if cfg.placement_policy == "random":
        random.shuffle(machine_choices)
    else:
        # Política padrão: ordena por maior espaço livre disponível (free_space)
        machine_choices.sort(key=lambda c: (c.free_bytes, -c.usage_pct), reverse=True)

    available_count = len(machine_choices)
    if available_count == 0:
        raise InsufficientStorageNodesError(
            "Nenhum nó/disco disponível com espaço suficiente para alocar o bloco."
        )

    warning: str | None = None
    if available_count < replication_target:
        if not cfg.allow_under_replicated_writes:
            raise InsufficientStorageNodesError(
                f"Máquinas disponíveis ({available_count}) insuficientes para o nível de "
                f"replicação exigido (N={replication_target}) e gravações sub-replicadas estão desabilitadas."
            )
        warning = (
            f"Apenas {available_count} máquina(s) distinta(s) disponível(is) para N={replication_target}. "
            f"O arquivo será gravado como SUB_REPLICATED (1 réplica por máquina disponível)."
        )

    chosen = machine_choices[:replication_target]
    targets: list[ReplicaTarget] = []
    for c in chosen:
        c.projected_used_bytes += block_size_bytes
        targets.append(
            ReplicaTarget(
                node_id=c.node_id,
                host_url=c.host_url,
                machine_id=c.machine_id,
                disk_id=c.disk_id,
            )
        )

    return targets, warning
