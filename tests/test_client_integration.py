"""Cliente + controlador PostgreSQL + cinco APIs reais de storage em transporte ASGI."""

from __future__ import annotations

import asyncio
import hashlib
import os
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from src.client.chunker import DecryptionError, IntegrityError
from src.client.transfer import ClientError, DriveClient
from src.common.config import Settings, get_settings
from src.controller.database import close_db, configure_database, init_db
from src.controller.main import app as controller_app
from src.controller.models import Base
from src.storage_node.disk_manager import DiskManager
from src.storage_node.main import app as storage_app
from src.storage_node.workers import NodeAgent


class ClusterTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.apps = {"controller": controller_app}
        self.failures = {}
        self.requests = []
        self.payloads = []

    async def handle_async_request(self, request):
        self.requests.append((request.method, request.url.host, request.url.path))
        if request.headers.get("content-type") == "application/json":
            import json

            self.payloads.append(json.loads(await request.aread()))
        mode = self.failures.get((request.method, request.url.host))
        if mode == "offline":
            raise httpx.ConnectError("Nó indisponível", request=request)
        if mode == "slow":
            await asyncio.sleep(30)
        if mode == "bad-ack":
            return httpx.Response(201, json={"block_id": "invalid"})
        if mode == "http-error":
            return httpx.Response(503, json={"detail": "Temporariamente indisponível"})
        response = await httpx.ASGITransport(
            app=self.apps[request.url.host]
        ).handle_async_request(request)
        if mode == "tamper" and response.status_code == 200:
            content = await response.aread()
            return httpx.Response(200, content=bytes([content[0] ^ 1]) + content[1:])
        if mode in ("truncated", "oversize") and response.status_code == 200:
            content = await response.aread()
            return httpx.Response(
                200, content=content[:-1] if mode == "truncated" else content + b"extra"
            )
        return response


@pytest.fixture
async def cluster(tmp_path, monkeypatch):
    url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://dcomp:dcomp@localhost:5433/dcomp_drive",
    )
    monkeypatch.setenv("DATABASE_URL", url)
    get_settings.cache_clear()
    engine, _ = configure_database(url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await init_db()
    transport = ClusterTransport()
    original_client = httpx.AsyncClient

    def routed_client(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", routed_client)
    async with original_client(transport=transport) as http:
        settings = Settings(
            chunk_size_mb=1,
            ENCRYPTION_KEY="ab" * 32,
            CONTROLLER_URL="http://controller",
        )
        nodes = {}
        for number in range(1, 6):
            node_id = f"node-{number}"
            app = FastAPI(exception_handlers=storage_app.exception_handlers)
            app.include_router(storage_app.router)
            disks = DiskManager(
                {f"{node_id}:{disk}": tmp_path / node_id / disk for disk in ("a", "b")}
            )
            agent = NodeAgent(
                node_id=node_id,
                machine_id=f"machine-{number}",
                host_url=f"http://{node_id}",
                settings=settings,
                disks=disks,
                http=http,
            )
            app.state.agent = agent
            transport.apps[node_id] = app
            nodes[node_id] = agent
            await agent.register()
        async with DriveClient(settings, http=http) as drive:
            yield SimpleNamespace(
                drive=drive, nodes=nodes, transport=transport, http=http
            )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await close_db()
    get_settings.cache_clear()


@pytest.mark.parametrize("encrypted", [False, True])
async def test_all_operations_multiblock_and_physical_delete(
    cluster, tmp_path, encrypted
):
    drive = cluster.drive
    data = bytes(range(256)) * (3 * 4096 + 1)
    source, target = tmp_path / "source", tmp_path / "download"
    source.write_bytes(data)
    assert (await drive.mkdir("/docs/a")).is_dir
    result = await drive.put(
        source, "/docs/a/file", encrypt=encrypted, replication_target=3
    )
    assert result.total_blocks == 4
    stat = await drive.stat("/docs/a/file")
    assert stat.size_bytes == len(data)
    assert stat.file_hash == hashlib.sha256(data).hexdigest()
    assert stat.is_encrypted == encrypted
    for block in stat.blocks:
        for replica in block.replicas:
            wire = (
                cluster.nodes[replica.node_id].disks.mount(replica.disk_id)
                / f"{block.block_id}.blk"
            ).read_bytes()
            assert hashlib.sha256(wire).hexdigest() == block.block_hash
            if encrypted:
                assert wire != data[: len(wire)]
    await drive.get(stat.path, target)
    assert target.read_bytes() == data
    assert (await drive.ls("/docs/a")).entries[0].name == "file"
    assert (await drive.mv(stat.path, "/docs/renamed")).path == "/docs/renamed"
    state = await drive.cluster_status()
    assert len(state["nodes"]) == 5 and "active_re_replications" in state
    assert (await drive.drain("node-5"))["status"] == "DRAINING"
    assert (await drive.rm("/docs/renamed")).scheduled_replica_deletions > 0
    for block in stat.blocks:
        for replica in block.replicas:
            assert not (
                cluster.nodes[replica.node_id].disks.mount(replica.disk_id)
                / f"{block.block_id}.blk"
            ).exists()
    with pytest.raises(ClientError, match="409"):
        await drive.rm("/docs")
    await drive.rm("/docs", recursive=True)


@pytest.mark.parametrize("encrypted", [False, True])
async def test_empty_file(cluster, tmp_path, encrypted):
    source, target = tmp_path / "empty", tmp_path / "out"
    source.touch()
    result = await cluster.drive.put(source, "/empty", encrypt=encrypted)
    assert result.total_blocks == 0
    await cluster.drive.get("/empty", target)
    assert target.read_bytes() == b""


@pytest.mark.parametrize(
    "mode",
    ["offline", "http-error", "tamper", "truncated", "oversize", "disk-corruption"],
)
async def test_fallback_and_real_corruption_recovery(cluster, tmp_path, mode):
    drive = cluster.drive
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"replica content" * 100)
    await drive.put(source, "/file", replication_target=3)
    metadata = await drive.stat("/file")
    block = metadata.blocks[0]
    replica = min(block.replicas, key=lambda r: r.node_id)
    if mode == "disk-corruption":
        path = (
            cluster.nodes[replica.node_id].disks.mount(replica.disk_id)
            / f"{block.block_id}.blk"
        )
        path.write_bytes(b"broken")
    else:
        cluster.transport.failures[("GET", replica.node_id)] = mode
    await drive.get("/file", target)
    assert target.read_bytes() == source.read_bytes()
    reports = [
        r for r in cluster.transport.requests if r[2] == "/blocks/report-corruption"
    ]
    if mode in ("offline", "http-error"):
        assert not reports
    else:
        assert reports
        assert any(r[2] == "/blocks/replicate" for r in cluster.transport.requests)
        recovered = await drive.stat("/file")
        assert len(recovered.blocks[0].replicas) >= 2
        assert replica.node_id not in {r.node_id for r in recovered.blocks[0].replicas}


@pytest.mark.parametrize("mode", ["offline", "bad-ack"])
async def test_failed_upload_never_commits_below_quorum(cluster, tmp_path, mode):
    source = tmp_path / "source"
    source.write_bytes(b"quorum")
    for node in cluster.nodes:
        cluster.transport.failures[("PUT", node)] = mode
    with pytest.raises(ClientError, match="Quórum"):
        await cluster.drive.put(source, "/incomplete")
    assert not any(r[2] == "/files/commit" for r in cluster.transport.requests)
    with pytest.raises(ClientError, match="409"):
        await cluster.drive.get("/incomplete", tmp_path / "out")


async def test_quorum_does_not_wait_for_slow_replica(cluster, tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"quorum available")
    cluster.transport.failures[("PUT", "node-1")] = "slow"
    result = await asyncio.wait_for(cluster.drive.put(source, "/quorum"), timeout=4)
    assert result.status.value == "SUB_REPLICATED"
    await cluster.drive.get("/quorum", tmp_path / "out")


async def test_wrong_key_and_complete_hash_preserve_destination(cluster, tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"authenticated data")
    target.write_bytes(b"existing destination")
    await cluster.drive.put(source, "/encrypted", encrypt=True)
    cluster.drive.settings.encryption_key = "cd" * 32
    before = len(
        [r for r in cluster.transport.requests if r[2] == "/blocks/report-corruption"]
    )
    with pytest.raises(DecryptionError):
        await cluster.drive.get("/encrypted", target)
    assert target.read_bytes() == b"existing destination"
    assert (
        len(
            [
                r
                for r in cluster.transport.requests
                if r[2] == "/blocks/report-corruption"
            ]
        )
        == before
    )
    cluster.drive.settings.encryption_key = "ab" * 32
    await cluster.drive.put(source, "/plain", encrypt=False)
    from sqlalchemy import update

    from src.controller.database import get_session_factory
    from src.controller.models import Entry

    async with get_session_factory()() as session:
        await session.execute(
            update(Entry).where(Entry.path == "/plain").values(file_hash="0" * 64)
        )
        await session.commit()
    with pytest.raises(IntegrityError, match="arquivo reconstruído"):
        await cluster.drive.get("/plain", target)
    assert target.read_bytes() == b"existing destination"
    assert not list(tmp_path.glob("*.part"))


async def test_controller_failure_is_explicit(cluster):
    cluster.transport.failures[("GET", "controller")] = "offline"
    with pytest.raises(ClientError, match="controlador"):
        await cluster.drive.ls()


async def test_all_replicas_fail_without_overwriting_destination(cluster, tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.write_bytes(b"original data")
    destination.write_bytes(b"preserved")
    await cluster.drive.put(source, "/file")
    for index in range(1, 6):
        cluster.transport.failures[("GET", f"node-{index}")] = "offline"
    with pytest.raises(ClientError, match="Nenhuma réplica"):
        await cluster.drive.get("/file", destination)
    assert destination.read_bytes() == b"preserved"
    assert not list(tmp_path.glob("*.part"))


async def test_failed_corruption_report_does_not_prevent_fallback(cluster, tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.write_bytes(b"valid content")
    await cluster.drive.put(source, "/file")
    metadata = await cluster.drive.stat("/file")
    replica = min(metadata.blocks[0].replicas, key=lambda r: r.node_id)
    cluster.transport.failures[("GET", replica.node_id)] = "tamper"
    cluster.transport.failures[("POST", "controller")] = "offline"
    await cluster.drive.get("/file", destination)
    assert destination.read_bytes() == source.read_bytes()
    assert any("reporte" in warning for warning in cluster.drive.warnings)


async def test_encryption_configuration_and_key_never_leave_client(cluster, tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.write_bytes(b"private original content")
    cluster.drive.settings.encryption_enabled = True
    await cluster.drive.put(source, "/configured")
    await cluster.drive.get("/configured", destination)
    metadata = await cluster.drive.stat("/configured")
    assert metadata.is_encrypted and destination.read_bytes() == source.read_bytes()
    import json

    payloads = json.dumps(cluster.transport.payloads)
    assert "ab" * 32 not in payloads
    assert "ENCRYPTION_KEY" not in payloads and "encryption_key" not in payloads


async def test_under_replicated_cluster_uses_controller_effective_quorum(
    cluster, tmp_path
):
    # Somente uma máquina continua elegível; o controlador reduz W de 2 para 1.
    for number in range(2, 6):
        await cluster.drive.drain(f"node-{number}")
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.write_bytes(b"single available machine")
    result = await cluster.drive.put(source, "/under-replicated")
    assert result.write_quorum_used == 1 and result.status.value == "SUB_REPLICATED"
    assert any("máquina" in warning for warning in cluster.drive.warnings)
    await cluster.drive.get("/under-replicated", destination)
    assert destination.read_bytes() == source.read_bytes()


@pytest.mark.parametrize(
    "is_encrypted,nonce,size,original",
    [
        (True, None, 20, 4),
        (True, "00" * 11, 20, 4),
        (True, "gg" * 12, 20, 4),
        (True, "00" * 12, 15, 0),
        (False, "00" * 12, 4, 4),
        (False, None, 4, 5),
    ],
)
async def test_allocation_rejects_incompatible_crypto_metadata(
    cluster, is_encrypted, nonce, size, original
):
    response = await cluster.http.post(
        "http://controller/files/allocate",
        json={
            "path": "/invalid",
            "size_bytes": original,
            "file_hash": "0" * 64,
            "is_encrypted": is_encrypted,
            "blocks": [
                {
                    "block_index": 0,
                    "size_bytes": size,
                    "block_hash": "0" * 64,
                    "encryption_nonce": nonce,
                }
            ],
        },
    )
    assert response.status_code == 400
