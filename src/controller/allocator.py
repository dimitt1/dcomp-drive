"""Algoritmo de posicionamento de réplicas de blocos (RF3, RF7).

Garante:
- Regra de não repetição: nunca duas réplicas do mesmo bloco no mesmo disco, nó ou máquina.
- Gerenciamento de espaço: discos com uso >= disk_usage_limit_pct ou sem espaço livre suficiente
  não recebem novas réplicas.
- Balanceamento multicritério: escolhe o disco mais livre dentro de cada máquina e ordena
  as máquinas por espaço livre (ou menor taxa de uso / aleatório conforme política),
  atualizando o espaço projetado a cada bloco alocado.
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

    def can_fit(self, block_size_bytes: int, usage_limit_pct: float) -> bool:
        """Verifica se o disco comporta `block_size_bytes` respeitando o limite percentual."""
        if self.total_bytes <= 0:
            return False
        if self.free_bytes < block_size_bytes:
            return False
        if self.usage_pct >= usage_limit_pct:
            return False
        projected_after = ((self.projected_used_bytes + block_size_bytes) / self.total_bytes) * 100.0
        if block_size_bytes > 0 and projected_after > usage_limit_pct:
            return False
        return True


async def get_eligible_disks(
    db: AsyncSession,
    settings: Settings | None = None,
) -> list[CandidateDisk]:
    """Busca todos os discos ativos pertencentes a nós ALIVE abaixo do limite de uso."""
    cfg = settings or get_settings()
    stmt = (
        select(NodeDisk)
        .join(StorageNode, NodeDisk.node_id == StorageNode.node_id)
        .options(selectinload(NodeDisk.node))
        .where(
            StorageNode.status == NodeStatus.ALIVE.value,
            NodeDisk.is_active.is_(True),
        )
        .order_by(NodeDisk.node_id.asc(), NodeDisk.disk_id.asc())
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
    - Espaço livre >= `block_size_bytes` e uso projetado dentro de `disk_usage_limit_pct`.
    """
    if replication_target < 1:
        raise ValueError("replication_target deve ser pelo menos 1.")

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
        and c.can_fit(block_size_bytes, cfg.disk_usage_limit_pct)
    ]

    # Agrupa por machine_id, escolhendo o melhor disco de cada máquina
    # Critério: maior espaço livre absoluto; desempate por menor percentual de uso e disk_id
    best_per_machine: dict[str, CandidateDisk] = {}
    for c in valid:
        current = best_per_machine.get(c.machine_id)
        if current is None or (c.free_bytes, -c.usage_pct, current.disk_id > c.disk_id) > (
            current.free_bytes,
            -current.usage_pct,
            False,
        ):
            best_per_machine[c.machine_id] = c

    machine_choices = list(best_per_machine.values())

    policy = (cfg.placement_policy or "free_space").lower()
    if policy == "random":
        random.shuffle(machine_choices)
    elif policy == "low_utilization":
        machine_choices.sort(key=lambda c: (c.usage_pct, -c.free_bytes, c.node_id, c.disk_id))
    else:
        # Política padrão (free_space): ordena por maior espaço livre disponível
        machine_choices.sort(
            key=lambda c: (-c.free_bytes, c.usage_pct, c.node_id, c.disk_id)
        )

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

