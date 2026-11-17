"""API do plano de dados de um nó de armazenamento."""

from __future__ import annotations

import asyncio
import errno
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, BinaryIO
from uuid import UUID

import httpx
from anyio import to_thread
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, HttpUrl
from starlette.background import BackgroundTask

from src.common.config import get_settings
from src.common.logger import get_logger
from src.storage_node.disk_manager import (
    BUFFER_SIZE,
    SHA256_PATTERN,
    BlockConflict,
    CorruptedBlock,
    DiskManager,
    HashMismatch,
    UnknownDisk,
)
from src.storage_node.workers import NodeAgent

logger = get_logger("storage_node.main")
BINARY = "application/octet-stream"


class ReplicationRequest(BaseModel):
    block_id: UUID
    source_disk_id: str
    target_node_url: HttpUrl
    target_disk_id: str
    expected_hash: str = Field(pattern=SHA256_PATTERN)


def read_mounts(value: str) -> dict[str, Path]:
    mounts: dict[str, Path] = {}
    for entry in value.split(","):
        disk_id, separator, directory = entry.partition("=")
        disk_id, directory = disk_id.strip(), directory.strip()
        if not separator or not disk_id or not directory or disk_id in mounts:
            raise ValueError("DISK_MOUNTS deve listar pares disk_id=diretório distintos.")
        mounts[disk_id] = Path(directory)
    return mounts


@asynccontextmanager
async def lifespan(app: FastAPI):
    node_id = os.environ["NODE_ID"]
    settings = get_settings()
    disks = DiskManager(read_mounts(os.environ["DISK_MOUNTS"]))
    async with httpx.AsyncClient(
        timeout=20.0, follow_redirects=False, trust_env=False
    ) as http:
        agent = NodeAgent(
            node_id=node_id,
            machine_id=os.getenv("MACHINE_ID", node_id),
            host_url=os.environ["HOST_URL"],
            settings=settings,
            disks=disks,
            http=http,
        )
        app.state.agent = agent
        async with asyncio.TaskGroup() as tasks:
            heartbeat = tasks.create_task(agent.heartbeat_loop())
            scrubbing = tasks.create_task(agent.scrub_loop())
            logger.info("storage_node_started", node_id=node_id, disk_ids=list(disks.mounts))
            try:
                yield
            finally:
                heartbeat.cancel()
                scrubbing.cancel()


app = FastAPI(title="DComp Drive — Nó de Armazenamento", lifespan=lifespan)


@app.exception_handler(UnknownDisk)
async def unknown_disk(_request: Request, exc: UnknownDisk) -> JSONResponse:
    return JSONResponse({"detail": f"Disco desconhecido: {exc}"}, status_code=404)


@app.exception_handler(OSError)
async def disk_error(_request: Request, exc: OSError) -> JSONResponse:
    logger.error("storage_io_failed", error=str(exc))
    status = 507 if exc.errno in (errno.ENOSPC, errno.EDQUOT) else 500
    return JSONResponse({"detail": "Falha de armazenamento local."}, status_code=status)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    agent: NodeAgent = request.app.state.agent
    return {"status": "ok", "component": "storage_node", "node_id": agent.node_id}


@app.put("/blocks/{block_id}", status_code=201)
async def put_block(
    request: Request,
    block_id: UUID,
    disk_id: str,
    expected_hash: Annotated[str, Query(pattern=SHA256_PATTERN)],
) -> dict:
    agent: NodeAgent = request.app.state.agent
    try:
        size = await agent.disks.write(disk_id, block_id, request.stream(), expected_hash)
    except HashMismatch as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except BlockConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"block_id": block_id, "disk_id": disk_id, "size_bytes": size}


async def open_block(
    agent: NodeAgent, disk_id: str, block_id: UUID
) -> tuple[BinaryIO, int, str]:
    try:
        return await to_thread.run_sync(agent.disks.open_verified, disk_id, block_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Bloco não encontrado.") from exc
    except CorruptedBlock as exc:
        await agent.report_corruption(disk_id, block_id, "storage_node")
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def read_chunks(block: BinaryIO) -> AsyncIterator[bytes]:
    while chunk := await to_thread.run_sync(block.read, BUFFER_SIZE):
        yield chunk


async def download_chunks(block: BinaryIO) -> AsyncIterator[bytes]:
    with block:
        async for chunk in read_chunks(block):
            yield chunk


@app.get("/blocks/{block_id}")
async def get_block(request: Request, block_id: UUID, disk_id: str) -> StreamingResponse:
    agent: NodeAgent = request.app.state.agent
    block, size, digest = await open_block(agent, disk_id, block_id)
    return StreamingResponse(
        download_chunks(block),
        media_type=BINARY,
        headers={"Content-Length": str(size), "X-Block-SHA256": digest},
        background=BackgroundTask(block.close),
    )


@app.delete("/blocks/{block_id}", status_code=204)
async def delete_block(request: Request, block_id: UUID, disk_id: str) -> Response:
    agent: NodeAgent = request.app.state.agent
    await to_thread.run_sync(agent.disks.delete, disk_id, block_id)
    return Response(status_code=204)


async def check_target(agent: NodeAgent, order: ReplicationRequest) -> None:
    # Uma ordem só pode enviar dados a um disco de outro nó registrado no controlador.
    try:
        response = await agent.http.get(agent.controller_endpoint("/cluster/status"))
        response.raise_for_status()
        nodes = response.json()["nodes"]
    except (httpx.HTTPError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=503, detail="Não foi possível confirmar o destino.") from exc

    target = str(order.target_node_url).rstrip("/")
    for node in nodes:
        if node["node_id"] != agent.node_id and node["host_url"].rstrip("/") == target:
            if any(disk["disk_id"] == order.target_disk_id for disk in node["disks"]):
                return
    raise HTTPException(status_code=422, detail="O disco de destino não pertence a outro nó do cluster.")


@app.post("/blocks/replicate")
async def replicate_block(request: Request, order: ReplicationRequest) -> dict:
    agent: NodeAgent = request.app.state.agent
    await check_target(agent, order)
    block, size, digest = await open_block(agent, order.source_disk_id, order.block_id)
    with block:
        if digest != order.expected_hash.lower():
            raise HTTPException(status_code=422, detail="Hash da origem diverge da ordem de cópia.")

        try:
            response = await agent.http.put(
                f"{str(order.target_node_url).rstrip('/')}/blocks/{order.block_id}",
                params={"disk_id": order.target_disk_id, "expected_hash": digest},
                headers={"Content-Type": BINARY, "Content-Length": str(size)},
                content=read_chunks(block),
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "replication_failed",
                block_id=order.block_id,
                target_disk_id=order.target_disk_id,
                error=str(exc),
            )
            raise HTTPException(status_code=502, detail="O destino não confirmou a réplica.") from exc

    return {"block_id": order.block_id, "target_disk_id": order.target_disk_id, "size_bytes": size}
