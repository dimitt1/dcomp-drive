"""Testes automatizados para Metadados, Persistência e Concorrência)."""

from __future__ import annotations

import hashlib
import os
from datetime import timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from src.common.config import get_settings
from src.common.schemas import EntryStatus
from src.controller.database import (
    close_db,
    configure_database,
    get_engine,
    get_session_factory,
    init_db,
)
from src.controller.main import app
from src.controller.models import Base, Entry, utc_now


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
async def client(monkeypatch):
    """Inicializa o schema no PostgreSQL (limpo por teste) e retorna o AsyncClient."""
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

    # Evita chamadas HTTP reais para Storage Nodes durante background tasks nos testes
    deleted_calls: list[tuple[str, str, str]] = []

    async def fake_delete_physical_replicas(replicas_to_delete):
        for block_id, disk_id, host_url in replicas_to_delete:
            deleted_calls.append((str(block_id), disk_id, host_url))

    monkeypatch.setattr(
        "src.controller.routes_metadata.delete_physical_replicas_on_nodes",
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


async def register_five_nodes(ac: AsyncClient) -> None:
    """Registra 5 nós de armazenamento em máquinas distintas com 2 discos cada."""
    for i in range(1, 6):
        payload = {
            "node_id": f"node-{i}",
            "host_url": f"http://node-{i}:800{i}",
            "machine_id": f"machine-{i}",
            "disks": [
                {
                    "disk_id": f"node-{i}:disk-a",
                    "mount_path": "/data/disk_a",
                    "total_bytes": 100 * 1024 * 1024,
                    "used_bytes": 10 * 1024 * 1024 * i,
                    "is_active": True,
                },
                {
                    "disk_id": f"node-{i}:disk-b",
                    "mount_path": "/data/disk_b",
                    "total_bytes": 100 * 1024 * 1024,
                    "used_bytes": 5 * 1024 * 1024 * i,
                    "is_active": True,
                },
            ],
            "existing_blocks": {},
        }
        resp = await ac.post("/nodes/register", json=payload)
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_directory_operations_and_move(client: AsyncClient) -> None:
    """Testa criação, listagem, movimentação e deleção de diretórios (RF1)."""
    # 1. Cria diretório aninhado /projetos/2026/relatorios
    resp = await client.post("/dirs", json={"path": "/projetos/2026/relatorios", "parents": True})
    assert resp.status_code == 201
    data = resp.json()
    assert data["path"] == "/projetos/2026/relatorios"
    assert data["is_dir"] is True
    assert data["status"] == EntryStatus.DIRECTORY.value

    # 2. Lista diretório raiz '/'
    ls_root = await client.get("/dirs", params={"path": "/"})
    assert ls_root.status_code == 200
    assert ls_root.json()["total_items"] == 1
    assert ls_root.json()["entries"][0]["path"] == "/projetos"

    # 3. Move /projetos/2026 para /projetos/2027
    mv_resp = await client.patch(
        "/entries/move",
        json={"source_path": "/projetos/2026", "destination_path": "/projetos/2027"},
    )
    assert mv_resp.status_code == 200
    assert mv_resp.json()["path"] == "/projetos/2027"

    # O subdiretório deve ter sido atualizado recursivamente para /projetos/2027/relatorios
    meta_sub = await client.get("/files/metadata", params={"path": "/projetos/2027/relatorios"})
    assert meta_sub.status_code == 200
    assert meta_sub.json()["parent_path"] == "/projetos/2027"

    # 4. Deleção sem recursive=true em diretório não vazio deve falhar com 409
    del_fail = await client.delete("/entries", params={"path": "/projetos", "recursive": False})
    assert del_fail.status_code == 409

    # 5. Deleção recursiva remove toda a subárvore
    del_ok = await client.delete("/entries", params={"path": "/projetos", "recursive": True})
    assert del_ok.status_code == 200
    assert del_ok.json()["deleted_entries_count"] == 3


@pytest.mark.asyncio
async def test_allocate_commit_and_non_repetition(client: AsyncClient) -> None:
    """Testa alocação de blocos, regra de não repetição (RF3), quórum W e commit."""
    await register_five_nodes(client)

    chunk0 = b"bloco_dados_0" * 100
    chunk1 = b"bloco_dados_1" * 100
    full_data = chunk0 + chunk1

    alloc_req = {
        "path": "/docs/manual.bin",
        "size_bytes": len(full_data),
        "file_hash": sha256_hex(full_data),
        "is_encrypted": False,
        "replication_target": 3,
        "blocks": [
            {"block_index": 0, "size_bytes": len(chunk0), "block_hash": sha256_hex(chunk0)},
            {"block_index": 1, "size_bytes": len(chunk1), "block_hash": sha256_hex(chunk1)},
        ],
    }
    alloc_resp = await client.post("/files/allocate", json=alloc_req)
    assert alloc_resp.status_code == 201
    alloc_data = alloc_resp.json()
    entry_id = alloc_data["entry_id"]
    assert alloc_data["status"] == EntryStatus.WRITING.value
    assert len(alloc_data["blocks"]) == 2

    # Verifica regra de não repetição: cada bloco deve ter 3 nós, 3 discos e 3 máquinas distintas
    for blk in alloc_data["blocks"]:
        targets = blk["targets"]
        assert len(targets) == 3
        assert len({t["disk_id"] for t in targets}) == 3
        assert len({t["node_id"] for t in targets}) == 3
        assert len({t["machine_id"] for t in targets}) == 3

    # Enquanto estiver WRITING, leitura com for_read=true deve ser bloqueada (RF8)
    read_while_writing = await client.get(
        "/files/metadata",
        params={"path": "/docs/manual.bin", "for_read": True},
    )
    assert read_while_writing.status_code == 409

    # Confirma apenas W=2 réplicas para o bloco 0 e N=3 réplicas para o bloco 1 -> SUB_REPLICATED
    b0 = alloc_data["blocks"][0]
    b1 = alloc_data["blocks"][1]
    confirmed = [
        {"block_id": b0["block_id"], "node_id": b0["targets"][0]["node_id"], "disk_id": b0["targets"][0]["disk_id"]},
        {"block_id": b0["block_id"], "node_id": b0["targets"][1]["node_id"], "disk_id": b0["targets"][1]["disk_id"]},
        {"block_id": b1["block_id"], "node_id": b1["targets"][0]["node_id"], "disk_id": b1["targets"][0]["disk_id"]},
        {"block_id": b1["block_id"], "node_id": b1["targets"][1]["node_id"], "disk_id": b1["targets"][1]["disk_id"]},
        {"block_id": b1["block_id"], "node_id": b1["targets"][2]["node_id"], "disk_id": b1["targets"][2]["disk_id"]},
    ]
    commit_resp = await client.post(
        "/files/commit",
        json={"entry_id": entry_id, "confirmed_replicas": confirmed},
    )
    assert commit_resp.status_code == 200
    commit_data = commit_resp.json()
    assert commit_data["status"] == EntryStatus.SUB_REPLICATED.value
    assert commit_data["fully_replicated_blocks"] == 1
    assert commit_data["sub_replicated_blocks"] == 1


@pytest.mark.asyncio
async def test_concurrency_lease_conflict_and_expiration(client: AsyncClient) -> None:
    """Verifica que escrita concorrente é bloqueada durante lease ativo e liberada após expirar (RF8)."""
    await register_five_nodes(client)
    content = b"teste_concorrencia"

    req = {
        "path": "/concorrente.txt",
        "size_bytes": len(content),
        "file_hash": sha256_hex(content),
        "is_encrypted": False,
        "replication_target": 3,
        "blocks": [
            {"block_index": 0, "size_bytes": len(content), "block_hash": sha256_hex(content)},
        ],
    }

    # Cliente 1 inicia gravação
    r1 = await client.post("/files/allocate", json=req)
    assert r1.status_code == 201

    # Cliente 2 tenta gravar no mesmo caminho enquanto lease está ativo -> 409 Conflict
    r2 = await client.post("/files/allocate", json=req)
    assert r2.status_code == 409

    # Simula expiração do lease no banco de dados
    session_factory = get_session_factory()
    async with session_factory() as session:
        stmt = select(Entry).where(Entry.path == "/concorrente.txt")
        entry = (await session.execute(stmt)).scalar_one()
        entry.lease_expires_at = utc_now() - timedelta(seconds=10)
        await session.commit()

    # Agora o Cliente 2 consegue alocar normalmente (o lease expirado do Cliente 1 é limpo)
    r3 = await client.post("/files/allocate", json=req)
    assert r3.status_code == 201


@pytest.mark.asyncio
async def test_report_corruption_and_file_delete_physical_blocks(
    client: AsyncClient,
    monkeypatch,
) -> None:
    """Testa reporte de corrupção (RF4) e disparo de deleção física ao remover arquivo."""
    await register_five_nodes(client)
    content = b"arquivo_integro_para_teste_corrupcao"

    # Evita chamada HTTP real na cópia nó-a-nó durante o teste
    replicated_calls: list[tuple[str, str, str]] = []

    async def fake_replicate_background(
        block_id,
        source_node_url,
        source_disk_id,
        target_node_id,
        target_node_url,
        target_disk_id,
        expected_hash,
    ):
        replicated_calls.append((str(block_id), target_node_id, target_disk_id))

    monkeypatch.setattr(
        "src.controller.routes_metadata._replicate_corrupted_block_background",
        fake_replicate_background,
    )

    alloc_resp = await client.post(
        "/files/allocate",
        json={
            "path": "/dados/importante.dat",
            "size_bytes": len(content),
            "file_hash": sha256_hex(content),
            "is_encrypted": False,
            "replication_target": 3,
            "blocks": [
                {"block_index": 0, "size_bytes": len(content), "block_hash": sha256_hex(content)},
            ],
        },
    )
    assert alloc_resp.status_code == 201
    alloc_data = alloc_resp.json()
    blk = alloc_data["blocks"][0]

    # Confirma todas as 3 réplicas -> HEALTHY
    confirmed = [
        {"block_id": blk["block_id"], "node_id": t["node_id"], "disk_id": t["disk_id"]}
        for t in blk["targets"]
    ]
    commit_resp = await client.post(
        "/files/commit",
        json={"entry_id": alloc_data["entry_id"], "confirmed_replicas": confirmed},
    )
    assert commit_resp.status_code == 200
    assert commit_resp.json()["status"] == EntryStatus.HEALTHY.value

    # Reporta corrupção na primeira réplica
    corrupted_target = blk["targets"][0]
    rep_resp = await client.post(
        "/blocks/report-corruption",
        json={
            "block_id": blk["block_id"],
            "node_id": corrupted_target["node_id"],
            "disk_id": corrupted_target["disk_id"],
            "reporter": "client",
        },
    )
    assert rep_resp.status_code == 200
    rep_data = rep_resp.json()
    assert rep_data["re_replication_triggered"] is True
    assert rep_data["file_status"] == EntryStatus.RECOVERING.value
    assert len(replicated_calls) == 1
    # O nó escolhido para re-replicação não pode ser nenhum dos 3 originais
    original_nodes = {t["node_id"] for t in blk["targets"]}
    assert replicated_calls[0][1] not in original_nodes

    # Remove o arquivo e verifica que as deleções físicas foram agendadas
    del_resp = await client.delete("/entries", params={"path": "/dados/importante.dat"})
    assert del_resp.status_code == 200
    assert del_resp.json()["scheduled_replica_deletions"] >= 2


@pytest.mark.asyncio
async def test_move_file_overwrite_and_under_replicated_allocation(client: AsyncClient) -> None:
    """Testa renomear arquivo, sobrescrever arquivo existente e alocação com menos máquinas que N."""
    # Registra apenas 2 máquinas para testar N=3 com allow_under_replicated_writes=true
    for i in range(1, 3):
        await client.post(
            "/nodes/register",
            json={
                "node_id": f"node-{i}",
                "host_url": f"http://node-{i}:800{i}",
                "machine_id": f"machine-{i}",
                "disks": [
                    {
                        "disk_id": f"node-{i}:disk-a",
                        "mount_path": "/data/disk_a",
                        "total_bytes": 50 * 1024 * 1024,
                        "used_bytes": 1024,
                        "is_active": True,
                    }
                ],
                "existing_blocks": {},
            },
        )

    data_v1 = b"versao_1"
    alloc1 = await client.post(
        "/files/allocate",
        json={
            "path": "/notas.txt",
            "size_bytes": len(data_v1),
            "file_hash": sha256_hex(data_v1),
            "is_encrypted": False,
            "replication_target": 3,
            "blocks": [{"block_index": 0, "size_bytes": len(data_v1), "block_hash": sha256_hex(data_v1)}],
        },
    )
    assert alloc1.status_code == 201
    a1 = alloc1.json()
    assert a1["under_replicated_warning"] is not None
    assert len(a1["blocks"][0]["targets"]) == 2

    # Faz commit das 2 réplicas disponíveis
    b0 = a1["blocks"][0]
    c1 = await client.post(
        "/files/commit",
        json={
            "entry_id": a1["entry_id"],
            "confirmed_replicas": [
                {"block_id": b0["block_id"], "node_id": t["node_id"], "disk_id": t["disk_id"]}
                for t in b0["targets"]
            ],
        },
    )
    assert c1.status_code == 200
    assert c1.json()["status"] == EntryStatus.SUB_REPLICATED.value

    # Move /notas.txt -> /arquivados/notas_v1.txt
    mv = await client.patch(
        "/entries/move",
        json={"source_path": "/notas.txt", "destination_path": "/arquivados/notas_v1.txt"},
    )
    assert mv.status_code == 200
    assert mv.json()["path"] == "/arquivados/notas_v1.txt"

    # Sobrescreve /arquivados/notas_v1.txt com nova versão
    data_v2 = b"versao_2_atualizada"
    alloc2 = await client.post(
        "/files/allocate",
        json={
            "path": "/arquivados/notas_v1.txt",
            "size_bytes": len(data_v2),
            "file_hash": sha256_hex(data_v2),
            "is_encrypted": False,
            "replication_target": 2,
            "blocks": [{"block_index": 0, "size_bytes": len(data_v2), "block_hash": sha256_hex(data_v2)}],
        },
    )
    assert alloc2.status_code == 201


