"""Registro, heartbeats e verificação periódica do nó."""

from __future__ import annotations

import asyncio
from uuid import UUID

import httpx
from anyio import to_thread

from src.common.config import Settings
from src.common.logger import get_logger
from src.common.schemas import NodeHeartbeatRequest, NodeRegisterRequest, ReportCorruptionRequest
from src.storage_node.disk_manager import CorruptedBlock, DiskManager

logger = get_logger("storage_node.workers")


class NodeAgent:
    def __init__(
        self,
        *,
        node_id: str,
        machine_id: str,
        host_url: str,
        settings: Settings,
        disks: DiskManager,
        http: httpx.AsyncClient,
    ) -> None:
        self.node_id = node_id
        self.machine_id = machine_id
        self.host_url = host_url
        self.settings = settings
        self.disks = disks
        self.http = http
        self.registered = asyncio.Event()

    def controller_endpoint(self, path: str) -> str:
        return f"{self.settings.controller_url.rstrip('/')}{path}"

    async def register(self) -> None:
        reports = await to_thread.run_sync(self.disks.disk_reports)
        inventory = await to_thread.run_sync(self.disks.verified_inventory)
        payload = NodeRegisterRequest(
            node_id=self.node_id,
            host_url=self.host_url,
            machine_id=self.machine_id,
            disks=reports,
            existing_blocks=inventory,
        ).model_dump(mode="json")
        payload["disks"] = reports
        response = await self.http.post(self.controller_endpoint("/nodes/register"), json=payload)
        response.raise_for_status()
        for disk_id, blocks in response.json().get("obsolete_blocks", {}).items():
            for value in blocks:
                block_id = UUID(value)
                if disk_id in inventory and block_id in inventory[disk_id]:
                    try:
                        await to_thread.run_sync(self.disks.delete, disk_id, block_id)
                    except OSError as exc:
                        logger.warning(
                            "obsolete_block_delete_failed",
                            disk_id=disk_id,
                            block_id=block_id,
                            error=str(exc),
                        )
        self.registered.set()
        logger.info("node_registered", node_id=self.node_id)

    async def heartbeat_loop(self) -> None:
        needs_registration = True
        while True:
            try:
                if needs_registration:
                    await self.register()
                    needs_registration = False
                else:
                    reports = await to_thread.run_sync(self.disks.disk_reports)
                    payload = NodeHeartbeatRequest(node_id=self.node_id, disks=reports).model_dump(
                        mode="json"
                    )
                    payload["disks"] = reports
                    response = await self.http.post(
                        self.controller_endpoint("/nodes/heartbeat"), json=payload
                    )
                    if response.status_code == 404:
                        self.registered.clear()
                        needs_registration = True
                        continue
                    response.raise_for_status()
            except (httpx.HTTPError, ValueError) as exc:
                self.registered.clear()
                needs_registration = True
                logger.warning("controller_contact_failed", node_id=self.node_id, error=str(exc))
            await asyncio.sleep(self.settings.heartbeat_interval_sec)

    async def report_corruption(self, disk_id: str, block_id: UUID, reporter: str) -> None:
        logger.warning(
            "block_corruption_detected",
            node_id=self.node_id,
            disk_id=disk_id,
            block_id=block_id,
            reporter=reporter,
        )
        payload = ReportCorruptionRequest(
            block_id=block_id,
            node_id=self.node_id,
            disk_id=disk_id,
            reporter=reporter,
        ).model_dump(mode="json")
        try:
            response = await self.http.post(
                self.controller_endpoint("/blocks/report-corruption"),
                json=payload,
                timeout=5.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("corruption_report_failed", block_id=block_id, error=str(exc))

    async def scrub_once(self) -> None:
        for disk_id in self.disks.mounts:
            try:
                block_ids = await to_thread.run_sync(self.disks.list_blocks, disk_id)
            except OSError as exc:
                logger.warning("scrubbing_disk_unavailable", disk_id=disk_id, error=str(exc))
                continue
            for block_id in block_ids:
                try:
                    await to_thread.run_sync(self.disks.verify, disk_id, block_id)
                except CorruptedBlock:
                    await self.report_corruption(disk_id, block_id, "scrubber")
                except FileNotFoundError:
                    # Uma exclusão pode ocorrer entre a listagem e a verificação.
                    continue
                except OSError as exc:
                    logger.warning("scrubbing_read_failed", block_id=block_id, error=str(exc))

    async def scrub_loop(self) -> None:
        while True:
            await self.registered.wait()
            await self.scrub_once()
            await asyncio.sleep(self.settings.scrubbing_interval_sec)
