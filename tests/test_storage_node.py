"""Regressões no plano de dados, sem dependência de PostgreSQL."""

from __future__ import annotations

import asyncio
import hashlib
import json
from uuid import uuid4

import httpx
import pytest

from src.common.config import Settings
from src.storage_node.disk_manager import BUFFER_SIZE, DiskManager, HashMismatch
from src.storage_node.main import app
from src.storage_node.workers import NodeAgent


@pytest.fixture
def anyio_backend():
    return "asyncio"


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def disks(tmp_path):
    return DiskManager({"node-1:disk-a": tmp_path / "disk-a", "node-1:disk-b": tmp_path / "disk-b"})


def make_agent(disks, http):
    return NodeAgent(
        node_id="node-1",
        machine_id="machine-1",
        host_url="http://node-1:8001",
        settings=Settings(CONTROLLER_URL="http://controller:8000"),
        disks=disks,
        http=http,
    )


@pytest.mark.anyio
async def test_partial_upload_does_not_publish_or_leave_temp_files(disks):
    disk_id, block_id = "node-1:disk-a", uuid4()

    async def interrupted_stream():
        yield b"partial"
        raise ConnectionError("client disconnected")

    with pytest.raises(ConnectionError):
        await disks.write(disk_id, block_id, interrupted_stream(), sha256(b"partial"))

    assert list(disks.mount(disk_id).iterdir()) == []


@pytest.mark.anyio
async def test_blocks_persist_and_bad_put_cannot_replace_verified_block(disks, monkeypatch):
    disk_id, second_disk, block_id = "node-1:disk-a", "node-1:disk-b", uuid4()
    payload = b"block" * (BUFFER_SIZE // 5 + 1)
    url = f"/blocks/{block_id}"
    valid_params = {"disk_id": disk_id, "expected_hash": sha256(payload)}

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))) as net:
        monkeypatch.setattr(app.state, "agent", make_agent(disks, net), raising=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://node-1") as client:
            bad = await client.put(url, params=valid_params, content=b"wrong")
            assert bad.status_code == 422
            assert (await client.get(url, params={"disk_id": disk_id})).status_code == 404

            result = await client.put(url, params=valid_params, content=payload)
            assert result.status_code == 201
            duplicate = await client.put(
                url, params={**valid_params, "disk_id": second_disk}, content=payload
            )
            assert duplicate.status_code == 409
            bad = await client.put(url, params=valid_params, content=b"wrong")
            assert bad.status_code == 422
            conflicting = await client.put(
                url, params={**valid_params, "expected_hash": sha256(b"wrong")}, content=b"wrong"
            )
            assert conflicting.status_code == 409

            restarted = DiskManager(disks.mounts)
            app.state.agent = make_agent(restarted, net)
            response = await client.get(url, params={"disk_id": disk_id})
            assert response.status_code == 200
            assert response.headers["X-Block-SHA256"] == sha256(payload)
            assert response.content == payload
            assert restarted.verified_inventory() == {disk_id: [block_id], second_disk: []}

            for _ in range(2):
                assert (await client.delete(url, params={"disk_id": disk_id})).status_code == 204
            assert list(restarted.mount(disk_id).iterdir()) == []


@pytest.mark.anyio
async def test_read_and_scrub_report_corruption_and_exclude_it_from_inventory(disks, monkeypatch):
    disk_id, block_id = "node-1:disk-a", uuid4()
    reports = []

    def controller(request):
        reports.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(controller)) as net:
        agent = make_agent(disks, net)
        monkeypatch.setattr(app.state, "agent", agent, raising=False)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://node-1"
        ) as client:
            params = {"disk_id": disk_id, "expected_hash": sha256(b"valid")}
            url = f"/blocks/{block_id}"
            assert (await client.put(url, params=params, content=b"valid")).status_code == 201
            (disks.mount(disk_id) / f"{block_id}.blk").write_bytes(b"broken")

            result = await client.get(url, params={"disk_id": disk_id})
            assert result.status_code == 422
            await agent.scrub_once()
            assert disks.verified_inventory()[disk_id] == []

            assert reports == [
                (
                    "/blocks/report-corruption",
                    {
                        "block_id": str(block_id),
                        "node_id": "node-1",
                        "disk_id": disk_id,
                        "reporter": reporter,
                    },
                )
                for reporter in ("storage_node", "scrubber")
            ]
            assert (await client.put(url, params=params, content=b"valid")).status_code == 201
            assert (await client.get(url, params={"disk_id": disk_id})).content == b"valid"


@pytest.mark.anyio
async def test_replication_copies_bytes_to_a_registered_disk(disks, tmp_path, monkeypatch):
    source_disk, target_disk = "node-1:disk-a", "node-2:disk-b"
    target = DiskManager({target_disk: tmp_path / "disk-target"})
    block_id, payload = uuid4(), b"replicated content"

    async def target_or_controller(request):
        if request.url.host == "controller":
            return httpx.Response(200, json={
                "nodes": [{
                    "node_id": "node-2",
                    "host_url": "http://node-2:8002",
                    "disks": [{"disk_id": target_disk}],
                }]
            })
        assert request.method == "PUT"
        assert request.url.path == f"/blocks/{block_id}"
        assert request.url.params["disk_id"] == target_disk
        assert request.headers["Content-Length"] == str(len(payload))
        try:
            await target.write(
                target_disk, block_id, request.stream, request.url.params["expected_hash"]
            )
        except HashMismatch:
            return httpx.Response(422)
        return httpx.Response(201)

    async with httpx.AsyncClient(transport=httpx.MockTransport(target_or_controller)) as net:
        monkeypatch.setattr(app.state, "agent", make_agent(disks, net), raising=False)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://node-1"
        ) as client:
            digest = sha256(payload)
            assert (await client.put(
                f"/blocks/{block_id}",
                params={"disk_id": source_disk, "expected_hash": digest},
                content=payload,
            )).status_code == 201

            order = {
                "block_id": str(block_id),
                "source_disk_id": source_disk,
                "target_node_url": "http://node-2:8002",
                "target_disk_id": target_disk,
                "expected_hash": digest,
            }
            rejected = await client.post(
                "/blocks/replicate", json={**order, "target_node_url": "http://unknown:8002"}
            )
            assert rejected.status_code == 422
            assert target.verified_inventory()[target_disk] == []

            result = await client.post("/blocks/replicate", json=order)
            assert result.status_code == 200, result.text
            assert target.verified_inventory()[target_disk] == [block_id]
            assert (target.mount(target_disk) / f"{block_id}.blk").read_bytes() == payload


@pytest.mark.anyio
async def test_heartbeat_reregisters_after_controller_loses_node(disks):
    disk_id, block_id = "node-1:disk-a", uuid4()
    events = []

    async def content():
        yield b"block"

    await disks.write(disk_id, block_id, content(), sha256(b"block"))

    async def controller(request):
        payload = json.loads(request.content)
        events.append((request.url.path, payload))
        if request.url.path == "/nodes/heartbeat" and len(events) == 2:
            return httpx.Response(404)
        if len(events) == 3:
            return httpx.Response(200, json={"obsolete_blocks": {disk_id: [str(block_id)]}})
        if len(events) == 4:
            completed.set()
        return httpx.Response(200, json={})

    completed = asyncio.Event()
    async with httpx.AsyncClient(transport=httpx.MockTransport(controller)) as net:
        agent = make_agent(disks, net)
        agent.settings.heartbeat_interval_sec = 0.01
        task = asyncio.create_task(agent.heartbeat_loop())
        try:
            await asyncio.wait_for(completed.wait(), timeout=3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert [path for path, _ in events[:4]] == [
        "/nodes/register", "/nodes/heartbeat", "/nodes/register", "/nodes/heartbeat"
    ]
    assert events[0][1]["existing_blocks"][disk_id] == [str(block_id)]
    report = events[1][1]["disks"][0]
    assert report["disk_id"] == disk_id
    assert report["total_bytes"] > 0 and report["used_bytes"] >= 0 and report["free_bytes"] >= 0
    assert disks.verified_inventory()[disk_id] == []
