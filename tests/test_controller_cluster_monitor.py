"""Testes automatizados para Cluster, Alocação, Detecção de Falhas e Re-replicação (Integrante 2 — RF3, RF5, RF6, RF7, RF11)."""

from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from src.common.config import Settings, get_settings
from src.common.schemas import EntryStatus, NodeStatus, ReplicaStatus
from src.controller.database import (
    close_db,
    configure_database,
    get_engine,
    get_session_factory,
    init_db,
)
from src.controller.main import app
from src.controller.models import Base, Block, BlockReplica, Entry, StorageNode, utc_now
from src.controller.monitor import (
    detect_node_failures,
    run_cluster_monitor_cycle,
    schedule_re_replications,
)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
async def client(monkeypatch):
    """Inicializa o schema no PostgreSQL limpo por teste e retorna o AsyncClient."""
    db_url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://dcomp:dcomp@localhost:5433/dcomp_drive",
    )
    monkeypatch.setenv("DATABASE_URL", db_url)
    get_settings.cache_clear()

    configure_database(db_url)
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await init_db()

    deleted_calls: list[tuple[str, str, str]] = []

    async def fake_delete_physical_replicas(replicas_to_delete):
        for block_id, disk_id, host_url in replicas_to_delete:
            deleted_calls.append((str(block_id), disk_id, host_url))

    monkeypatch.setattr(
        "src.controller.routes_metadata.delete_physical_replicas_on_nodes",
        fake_delete_physical_replicas,
    )
    monkeypatch.setattr(
        "src.controller.monitor.delete_physical_replicas_on_nodes",
        fake_delete_physical_replicas,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        ac.deleted_calls = deleted_calls  # type: ignore[attr-defined]
        yield ac

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await init_db()
    await close_db()


async def register_node_helper(
    ac: AsyncClient,
    node_id: str,
    machine_id: str,
    disks: list[dict],
    existing_blocks: dict | None = None,
) -> dict:
    resp = await ac.post(
        "/nodes/register",
        json={
            "node_id": node_id,
            "host_url": f"http://{node_id}:8000",
            "machine_id": machine_id,
            "disks": disks,
            "existing_blocks": existing_blocks or {},
        },
    )
    assert resp.status_code == 200
    return resp.json()


@pytest.mark.asyncio
async def test_allocator_disk_usage_limit_and_same_machine_exclusion(
    client: AsyncClient,
    monkeypatch,
) -> None:
    """Garante limite de 90% de disco, escolha do disco mais livre e não repetição de máquina (RF3, RF7)."""
    monkeypatch.setenv("DISK_USAGE_LIMIT_PCT", "90.0")
    monkeypatch.setenv("ALLOW_UNDER_REPLICATED_WRITES", "false")
    get_settings.cache_clear()

    total = 100 * 1024 * 1024
    # node-1 e node-2 compartilham a mesma máquina (machine-1)
    await register_node_helper(
        client,
        "node-1",
        "machine-1",
        [
            {"disk_id": "node-1:disk-a", "mount_path": "/d/a", "total_bytes": total, "used_bytes": 10 * 1024 * 1024, "is_active": True},
        ],
    )
    await register_node_helper(
        client,
        "node-2",
        "machine-1",
        [
            {"disk_id": "node-2:disk-a", "mount_path": "/d/a", "total_bytes": total, "used_bytes": 5 * 1024 * 1024, "is_active": True},
        ],
    )
    # node-3 em machine-2 com disk-a acima de 90% (95%) e disk-b com 20%
    await register_node_helper(
        client,
        "node-3",
        "machine-2",
        [
            {"disk_id": "node-3:disk-a", "mount_path": "/d/a", "total_bytes": total, "used_bytes": 95 * 1024 * 1024, "is_active": True},
            {"disk_id": "node-3:disk-b", "mount_path": "/d/b", "total_bytes": total, "used_bytes": 20 * 1024 * 1024, "is_active": True},
        ],
    )

    chunk = b"x" * 1024
    # Temos 3 nós mas apenas 2 máquinas distintas (machine-1 e machine-2).
    # Com N=3 e allow_under_replicated_writes=false, deve rejeitar com 503.
    fail_resp = await client.post(
        "/files/allocate",
        json={
            "path": "/rejeitado.bin",
            "size_bytes": len(chunk),
            "file_hash": sha256_hex(chunk),
            "is_encrypted": False,
            "replication_target": 3,
            "blocks": [{"block_index": 0, "size_bytes": len(chunk), "block_hash": sha256_hex(chunk)}],
        },
    )
    assert fail_resp.status_code == 503

    # Registra node-4 em machine-3 com disco em 89%, mas bloco de 2 MiB ultrapassaria 90% -> deve ser filtrado!
    await register_node_helper(
        client,
        "node-4",
        "machine-3",
        [
            {"disk_id": "node-4:disk-a", "mount_path": "/d/a", "total_bytes": total, "used_bytes": 89 * 1024 * 1024, "is_active": True},
        ],
    )
    big_chunk = b"y" * (2 * 1024 * 1024)
    over_limit_resp = await client.post(
        "/files/allocate",
        json={
            "path": "/quase_cheio.bin",
            "size_bytes": len(big_chunk),
            "file_hash": sha256_hex(big_chunk),
            "is_encrypted": False,
            "replication_target": 3,
            "blocks": [{"block_index": 0, "size_bytes": len(big_chunk), "block_hash": sha256_hex(big_chunk)}],
        },
    )
    assert over_limit_resp.status_code == 503

    # Para o bloco pequeno (1 KiB), node-4:disk-a permanece abaixo de 90% e a alocação N=3 sucede
    ok_resp = await client.post(
        "/files/allocate",
        json={
            "path": "/aceito.bin",
            "size_bytes": len(chunk),
            "file_hash": sha256_hex(chunk),
            "is_encrypted": False,
            "replication_target": 3,
            "blocks": [{"block_index": 0, "size_bytes": len(chunk), "block_hash": sha256_hex(chunk)}],
        },
    )
    assert ok_resp.status_code == 201
    targets = ok_resp.json()["blocks"][0]["targets"]
    assert len(targets) == 3
    chosen_disks = {t["disk_id"] for t in targets}
    chosen_machines = {t["machine_id"] for t in targets}
    assert chosen_machines == {"machine-1", "machine-2", "machine-3"}
    # Na machine-1 deve ter escolhido node-2:disk-a (95MB livres > 90MB livres do node-1)
    # Na machine-2 deve ter escolhido node-3:disk-b (pois disk-a tem 95% de uso)
    assert chosen_disks == {"node-2:disk-a", "node-3:disk-b", "node-4:disk-a"}


@pytest.mark.asyncio
async def test_failure_detection_suspect_dead_and_priority_re_replication(
    client: AsyncClient,
    monkeypatch,
) -> None:
    """Testa transição ALIVE -> SUSPECT -> DEAD, priorização por menor nº de réplicas vivas e limite de concorrência (RF5)."""
    total = 100 * 1024 * 1024
    initial_used = {
        1: 1 * 1024 * 1024,
        2: 40 * 1024 * 1024,
        3: 2 * 1024 * 1024,
        4: 3 * 1024 * 1024,
        5: 50 * 1024 * 1024,
    }
    for i in range(1, 6):
        await register_node_helper(
            client,
            f"node-{i}",
            f"machine-{i}",
            [
                {
                    "disk_id": f"node-{i}:disk-a",
                    "mount_path": "/data/disk_a",
                    "total_bytes": total,
                    "used_bytes": initial_used[i],
                    "is_active": True,
                }
            ],
        )

    # Cria 2 arquivos com N=3:
    # - /arq_duas_vivas.bin: alocado em node-1, node-3, node-4 (perderá node-1 -> ficará com 2 réplicas vivas)
    # - /arq_uma_viva.bin: alocado em node-1, node-2, node-3 (perderá node-1 e node-2 -> ficará com 1 réplica viva -> MAIOR PRIORIDADE)
    data1 = b"arquivo_com_duas_replicas_vivas"
    data2 = b"arquivo_critico_com_uma_replica_viva"

    alloc1 = (
        await client.post(
            "/files/allocate",
            json={
                "path": "/arq_duas_vivas.bin",
                "size_bytes": len(data1),
                "file_hash": sha256_hex(data1),
                "is_encrypted": False,
                "replication_target": 3,
                "blocks": [{"block_index": 0, "size_bytes": len(data1), "block_hash": sha256_hex(data1)}],
            },
        )
    ).json()
    b_two = alloc1["blocks"][0]
    assert {t["node_id"] for t in b_two["targets"]} == {"node-1", "node-3", "node-4"}
    await client.post(
        "/files/commit",
        json={
            "entry_id": alloc1["entry_id"],
            "confirmed_replicas": [
                {"block_id": b_two["block_id"], "node_id": t["node_id"], "disk_id": t["disk_id"]}
                for t in b_two["targets"]
            ],
        },
    )

    # Atualiza heartbeat de node-2 e node-4 para que /arq_uma_viva.bin seja alocado em node-1, node-2, node-3
    await client.post(
        "/nodes/heartbeat",
        json={
            "node_id": "node-2",
            "disks": [{"disk_id": "node-2:disk-a", "mount_path": "/data/disk_a", "total_bytes": total, "used_bytes": 1 * 1024 * 1024, "is_active": True}],
        },
    )
    await client.post(
        "/nodes/heartbeat",
        json={
            "node_id": "node-4",
            "disks": [{"disk_id": "node-4:disk-a", "mount_path": "/data/disk_a", "total_bytes": total, "used_bytes": 30 * 1024 * 1024, "is_active": True}],
        },
    )

    alloc2 = (
        await client.post(
            "/files/allocate",
            json={
                "path": "/arq_uma_viva.bin",
                "size_bytes": len(data2),
                "file_hash": sha256_hex(data2),
                "is_encrypted": False,
                "replication_target": 3,
                "blocks": [{"block_index": 0, "size_bytes": len(data2), "block_hash": sha256_hex(data2)}],
            },
        )
    ).json()
    b_one = alloc2["blocks"][0]
    assert {t["node_id"] for t in b_one["targets"]} == {"node-1", "node-2", "node-3"}
    await client.post(
        "/files/commit",
        json={
            "entry_id": alloc2["entry_id"],
            "confirmed_replicas": [
                {"block_id": b_one["block_id"], "node_id": t["node_id"], "disk_id": t["disk_id"]}
                for t in b_one["targets"]
            ],
        },
    )

    session_factory = get_session_factory()
    now = utc_now()

    # 1. Simula atraso de 12s nos heartbeats de node-1 e node-2 (>= suspect_timeout=10s, < heartbeat_timeout=15s)
    async with session_factory() as session:
        nodes = (
            await session.execute(
                select(StorageNode).where(StorageNode.node_id.in_(["node-1", "node-2"]))
            )
        ).scalars().all()
        for n in nodes:
            n.last_heartbeat = now - timedelta(seconds=12)
        await session.commit()

    settings = Settings(
        suspect_timeout_sec=10,
        heartbeat_timeout_sec=15,
        max_concurrent_replications=1,
        disk_usage_limit_pct=90.0,
    )

    # Durante estado SUSPECT, nós são marcados SUSPECT e arquivos SUB_REPLICATED, mas ainda sem disparar cópia
    cycle_suspect = await run_cluster_monitor_cycle(
        settings=settings,
        session_factory=session_factory,
        now=now,
    )
    assert set(cycle_suspect["newly_suspect"]) == {"node-1", "node-2"}
    assert cycle_suspect["newly_dead"] == []
    assert cycle_suspect["scheduled_replications"] == 0

    meta_one = (await client.get("/files/metadata", params={"path": "/arq_uma_viva.bin"})).json()
    assert meta_one["status"] == EntryStatus.SUB_REPLICATED.value

    # 2. Agora avança o atraso de node-1 e node-2 para 18s (>= heartbeat_timeout=15s -> DEAD)
    async with session_factory() as session:
        nodes = (
            await session.execute(
                select(StorageNode).where(StorageNode.node_id.in_(["node-1", "node-2"]))
            )
        ).scalars().all()
        for n in nodes:
            n.last_heartbeat = now - timedelta(seconds=18)
        await session.commit()

    replicate_requests: list[dict] = []

    class FakeHttpClient:
        async def post(self, url: str, json: dict):
            replicate_requests.append({"url": url, **json})

            class Resp:
                status_code = 200
                text = "OK"

            return Resp()

    fake_http = FakeHttpClient()

    # Com max_concurrent_replications=1, o primeiro ciclo DEVE priorizar o bloco de /arq_uma_viva.bin (apenas 1 réplica viva em node-3)
    cycle_dead_1 = await run_cluster_monitor_cycle(
        settings=settings,
        session_factory=session_factory,
        http_client=fake_http,  # type: ignore[arg-type]
        now=now,
    )
    assert set(cycle_dead_1["newly_dead"]) == {"node-1", "node-2"}
    assert cycle_dead_1["scheduled_replications"] == 1
    assert cycle_dead_1["completed_replications"] == 1
    assert replicate_requests[0]["block_id"] == b_one["block_id"]
    assert replicate_requests[0]["source_disk_id"] == "node-3:disk-a"
    assert replicate_requests[0]["target_disk_id"] in {"node-4:disk-a", "node-5:disk-a"}

    # Executa mais um ciclo com max_concurrent_replications=4 para concluir as 2 réplicas restantes
    settings_fast = Settings(
        suspect_timeout_sec=10,
        heartbeat_timeout_sec=15,
        max_concurrent_replications=4,
        disk_usage_limit_pct=90.0,
    )
    cycle_dead_2 = await run_cluster_monitor_cycle(
        settings=settings_fast,
        session_factory=session_factory,
        http_client=fake_http,  # type: ignore[arg-type]
        now=now,
    )
    assert cycle_dead_2["completed_replications"] == 2

    # Ambos os arquivos devem estar HEALTHY com 3 réplicas VERIFIED nas 3 máquinas vivas (node-3, node-4, node-5)
    for path in ("/arq_uma_viva.bin", "/arq_duas_vivas.bin"):
        meta = (
            await client.get(
                "/files/metadata",
                params={"path": path, "include_all_replicas": True},
            )
        ).json()
        assert meta["status"] == EntryStatus.HEALTHY.value
        reps = meta["blocks"][0]["replicas"]
        assert len(reps) == 3
        assert {r["node_id"] for r in reps} == {"node-3", "node-4", "node-5"}
        assert all(r["status"] == ReplicaStatus.VERIFIED.value for r in reps)


@pytest.mark.asyncio
async def test_planned_drain_migration_and_node_reconciliation_on_return(
    client: AsyncClient,
) -> None:
    """Testa esvaziamento planejado (DRAINING) com remoção física pós-cópia e reconciliação de nó que retorna (RF6, RF7)."""
    total = 100 * 1024 * 1024
    for i in range(1, 5):
        await register_node_helper(
            client,
            f"node-{i}",
            f"machine-{i}",
            [
                {
                    "disk_id": f"node-{i}:disk-a",
                    "mount_path": "/data/disk_a",
                    "total_bytes": total,
                    "used_bytes": i * 1024 * 1024,
                    "is_active": True,
                }
            ],
        )

    payload = b"bloco_para_teste_de_drain_e_reconciliacao"
    alloc = (
        await client.post(
            "/files/allocate",
            json={
                "path": "/drain_test.bin",
                "size_bytes": len(payload),
                "file_hash": sha256_hex(payload),
                "is_encrypted": False,
                "replication_target": 3,
                "blocks": [{"block_index": 0, "size_bytes": len(payload), "block_hash": sha256_hex(payload)}],
            },
        )
    ).json()
    blk = alloc["blocks"][0]
    block_id = blk["block_id"]

    # Confirma réplicas em node-1, node-2, node-3
    await client.post(
        "/files/commit",
        json={
            "entry_id": alloc["entry_id"],
            "confirmed_replicas": [
                {"block_id": block_id, "node_id": "node-1", "disk_id": "node-1:disk-a"},
                {"block_id": block_id, "node_id": "node-2", "disk_id": "node-2:disk-a"},
                {"block_id": block_id, "node_id": "node-3", "disk_id": "node-3:disk-a"},
            ],
        },
    )

    # Aciona drain em node-1
    drain_resp = await client.post("/nodes/node-1/drain")
    assert drain_resp.status_code == 200
    assert drain_resp.json()["status"] == NodeStatus.DRAINING.value
    assert drain_resp.json()["replicas_to_migrate"] == 1

    class FakeHttpClient:
        async def post(self, url: str, json: dict):
            class Resp:
                status_code = 200
                text = "OK"

            return Resp()

    # Ciclo do monitor deve migrar a réplica de node-1 (DRAINING) para node-4 (ALIVE) e apagar bloco físico em node-1
    cycle = await run_cluster_monitor_cycle(
        settings=Settings(disk_usage_limit_pct=90.0),
        session_factory=get_session_factory(),
        http_client=FakeHttpClient(),  # type: ignore[arg-type]
    )
    assert cycle["completed_replications"] == 1

    meta = (
        await client.get(
            "/files/metadata",
            params={"path": "/drain_test.bin", "include_all_replicas": True},
        )
    ).json()
    assert meta["status"] == EntryStatus.HEALTHY.value
    assert {r["node_id"] for r in meta["blocks"][0]["replicas"]} == {"node-2", "node-3", "node-4"}
    assert any(
        call[0] == block_id and call[1] == "node-1:disk-a"
        for call in client.deleted_calls  # type: ignore[attr-defined]
    )

    # Agora simula um nó que estava DEAD (ex: node-5) voltando e reportando:
    # 1) block_id (que já possui 3 réplicas vivas em node-2, node-3, node-4 -> deve ser marcado obsoleto!)
    # 2) um bloco inexistente fantasma -> também deve ser marcado obsoleto!
    phantom_id = str(uuid4())
    reg_node5 = await register_node_helper(
        client,
        "node-5",
        "machine-5",
        [
            {
                "disk_id": "node-5:disk-a",
                "mount_path": "/data/disk_a",
                "total_bytes": total,
                "used_bytes": 1024,
                "is_active": True,
            }
        ],
        existing_blocks={"node-5:disk-a": [block_id, phantom_id]},
    )
    assert reg_node5["reconciled_blocks"] == 0
    assert set(reg_node5["obsolete_blocks"]["node-5:disk-a"]) == {block_id, phantom_id}

