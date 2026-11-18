"""Ponta a ponta por TCP: CLI em subprocessos, controlador, PostgreSQL e cinco nós."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.fixture
def live_cluster(tmp_path):
    database = os.getenv("TEST_DATABASE_URL")
    if not database:
        pytest.skip("Defina TEST_DATABASE_URL para um PostgreSQL exclusivo de testes.")
    controller = f"http://127.0.0.1:{unused_port()}"
    config = tmp_path / "client.yaml"
    config.write_text(
        "chunk_size_mb: 4\nreplication_factor: 3\nwrite_quorum: 3\nencryption_enabled: false\n",
        encoding="utf-8",
    )
    env = dict(
        os.environ,
        DATABASE_URL=database,
        CONTROLLER_URL=controller,
        CONFIG_PATH=str(config),
        ENCRYPTION_KEY="ab" * 32,
        PYTHONIOENCODING="utf-8",
        HEARTBEAT_INTERVAL_SEC="1",
        SCRUBBING_INTERVAL_SEC="3600",
    )
    reset_code = """
import asyncio
from src.controller.database import configure_database, close_db
from src.controller.models import Base
async def reset():
    engine, _ = configure_database()
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await close_db()
asyncio.run(reset())
"""
    subprocess.run(
        [sys.executable, "-c", reset_code],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=True,
        timeout=30,
    )
    reset = """
import asyncio
from src.controller.database import configure_database, init_db, close_db
from src.controller.models import Base
async def reset():
    engine, _ = configure_database()
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await init_db()
    await close_db()
asyncio.run(reset())
"""
    subprocess.run(
        [sys.executable, "-c", reset],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=True,
        timeout=30,
    )
    processes, logs, mounts = {}, [], {}

    def start(name, module, url, extra=None):
        log = (tmp_path / f"{name}.log").open("wb")
        logs.append(log)
        processes[name] = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                module,
                "--host",
                "127.0.0.1",
                "--port",
                str(httpx.URL(url).port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env={**env, **(extra or {})},
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )

    def wait_ready(check):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if check():
                    return
            except (httpx.HTTPError, ValueError, KeyError):
                pass
            if any(p.poll() is not None for p in processes.values()):
                break
            time.sleep(0.1)
        details = "\n".join(
            p.read_text(encoding="utf-8", errors="replace")[-5000:]
            for p in tmp_path.glob("*.log")
        )
        pytest.fail(f"Cluster não iniciou:\n{details}")

    def cli(*args, success=True, json_output=True):
        command = [
            sys.executable,
            "-m",
            "src.client",
            "--controller-url",
            controller,
            "--timeout",
            "5",
            "--config",
            str(config),
        ]
        if json_output:
            command.append("--json")
        result = subprocess.run(
            [*command, *map(str, args)],
            check=False,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=90,
        )
        if success:
            assert result.returncode == 0, result.stdout + result.stderr
            return json.loads(result.stdout) if json_output else result.stdout
        assert result.returncode != 0
        return result

    try:
        start("controller", "src.controller.main:app", controller)
        with httpx.Client(timeout=2, trust_env=False) as net:
            wait_ready(lambda: net.get(controller + "/health").is_success)
            for index in range(1, 6):
                name = f"node-{index}"
                url = f"http://127.0.0.1:{unused_port()}"
                mounts[name] = {
                    f"{name}:disk-{suffix}": tmp_path / name / f"disk-{suffix}"
                    for suffix in ("a", "b")
                }
                start(
                    name,
                    "src.storage_node.main:app",
                    url,
                    {
                        "NODE_ID": name,
                        "MACHINE_ID": f"machine-{index}",
                        "HOST_URL": url,
                        "DISK_MOUNTS": ",".join(
                            f"{disk}={mount}" for disk, mount in mounts[name].items()
                        ),
                    },
                )
            wait_ready(
                lambda: (
                    len(net.get(controller + "/cluster/status").json()["nodes"]) == 5
                )
            )
        yield cli, processes, mounts, env
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
        for process in processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in logs:
            log.close()


def test_all_cli_commands_large_files_encryption_failures_and_recovery(
    live_cluster, tmp_path
):
    cli, processes, mounts, env = live_cluster
    source, output = tmp_path / "large.bin", tmp_path / "download.bin"
    digest = hashlib.sha256()
    # 64 MiB + 31 bytes, sem carregar o arquivo inteiro na memória do teste.
    with source.open("wb") as file:
        for index in range(64):
            piece = bytes([index]) * 1024 * 1024
            file.write(piece)
            digest.update(piece)
        file.write(b"x" * 31)
        digest.update(b"x" * 31)
    expected_hash = digest.hexdigest()
    cli("mkdir", "/e2e/nested")
    for encrypted in (False, True):
        path = f"/e2e/nested/{'encrypted' if encrypted else 'plain'}.bin"
        result = cli("put", source, path, "--encrypt" if encrypted else "--no-encrypt")
        assert result["total_blocks"] == 17
        assert result["status"] == "HEALTHY"
        metadata = cli("stat", path)
        assert metadata["file_hash"] == expected_hash
        assert metadata["size_bytes"] == source.stat().st_size
        assert metadata["is_encrypted"] is encrypted
        for block in metadata["blocks"]:
            assert len(block["replicas"]) == 3
            for replica in block["replicas"]:
                wire = (
                    mounts[replica["node_id"]][replica["disk_id"]]
                    / f"{block['block_id']}.blk"
                ).read_bytes()
                assert hashlib.sha256(wire).hexdigest() == block["block_hash"]
        cli("get", path, output)
        assert hashlib.sha256(output.read_bytes()).hexdigest() == expected_hash
        listing = cli("ls", "/e2e/nested")
        assert path in {entry["path"] for entry in listing["entries"]}
    assert "Cluster" in cli("cluster-status", json_output=False)
    assert "SHA-256" in cli("stat", path, json_output=False)
    assert "plain.bin" in cli("ls", "/e2e/nested", json_output=False)
    assert cli("cluster-status")["active_re_replications"] == 0
    cli("mv", "/e2e/nested/plain.bin", "/e2e/moved.bin")
    cli("stat", "/e2e/nested/plain.bin", success=False)
    cli("rm", "/e2e/moved.bin")
    cli("rm", "/e2e", success=False)
    # Download com nó que caiu: metadados ainda o anunciam como ALIVE.
    metadata = cli("stat", path)
    replica = min(metadata["blocks"][0]["replicas"], key=lambda r: r["node_id"])
    node = replica["node_id"]
    processes[node].terminate()
    processes[node].wait(timeout=10)
    cli("get", path, output)
    assert hashlib.sha256(output.read_bytes()).hexdigest() == expected_hash
    assert cli("drain", node)["status"] == "DRAINING"
    # Corrupção real em disco, reporte e cópia nó-a-nó para um nó substituto.
    metadata = cli("stat", path)
    block = metadata["blocks"][0]
    damaged = next(r for r in block["replicas"] if r["node_id"] != node)
    (
        mounts[damaged["node_id"]][damaged["disk_id"]] / f"{block['block_id']}.blk"
    ).write_bytes(b"bit rot")
    cli("get", path, output)
    assert hashlib.sha256(output.read_bytes()).hexdigest() == expected_hash
    recovered = cli("stat", path)
    assert damaged["node_id"] not in {
        r["node_id"] for r in recovered["blocks"][0]["replicas"]
    }
    assert len(recovered["blocks"][0]["replicas"]) == 3
    # Chave errada não destrói o destino e não acusa as réplicas íntegras.
    env["ENCRYPTION_KEY"] = "cd" * 32
    failed = cli("get", path, output, success=False)
    assert "AES-GCM" in failed.stderr
    assert hashlib.sha256(output.read_bytes()).hexdigest() == expected_hash
    env["ENCRYPTION_KEY"] = "ab" * 32
    cli("rm", "/e2e", "--recursive")
    assert cli("ls")["entries"] == []
    processes["controller"].terminate()
    processes["controller"].wait(timeout=10)
    cli("cluster-status", success=False)
