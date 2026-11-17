"""Armazenamento local e verificação de integridade dos blocos."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from collections.abc import AsyncIterable
from contextlib import suppress
from pathlib import Path
from threading import Lock
from typing import BinaryIO
from uuid import UUID

from anyio import to_thread

from src.common.logger import get_logger

logger = get_logger("storage_node.disks")
BUFFER_SIZE = 1024 * 1024
SHA256_PATTERN = r"^[0-9a-fA-F]{64}$"


class UnknownDisk(Exception):
    """O disco informado não pertence a este nó."""


class HashMismatch(Exception):
    """O conteúdo recebido não corresponde ao hash esperado."""


class CorruptedBlock(Exception):
    """O conteúdo armazenado ou o hash persistido está inválido."""


class BlockConflict(Exception):
    """O bloco já existe no nó com outro disco ou hash."""


class DiskManager:
    def __init__(self, mounts: dict[str, Path]) -> None:
        self.mounts = {disk_id: path.resolve() for disk_id, path in mounts.items()}
        if not self.mounts or len(set(self.mounts.values())) != len(self.mounts):
            raise ValueError("Configure pelo menos um disco e use um diretório distinto por disco.")
        self._locks = tuple(Lock() for _ in range(64))
        for path in self.mounts.values():
            path.mkdir(parents=True, exist_ok=True)

    def mount(self, disk_id: str) -> Path:
        try:
            return self.mounts[disk_id]
        except KeyError as exc:
            raise UnknownDisk(disk_id) from exc

    def _lock(self, block_id: UUID):
        return self._locks[block_id.int % len(self._locks)]

    def disk_reports(self) -> list[dict]:
        reports = []
        for disk_id, mount in self.mounts.items():
            try:
                total, used, free = shutil.disk_usage(mount)
                active = mount.is_dir()
            except OSError:
                total = used = free = 0
                active = False
            reports.append(
                {
                    "disk_id": disk_id,
                    "mount_path": str(mount),
                    "total_bytes": total,
                    "used_bytes": used,
                    "free_bytes": free,
                    "is_active": active,
                }
            )
        return reports

    def list_blocks(self, disk_id: str) -> list[UUID]:
        block_ids = set()
        for path in self.mount(disk_id).iterdir():
            if path.suffix == ".blk" and path.is_file():
                with suppress(ValueError):
                    block_id = UUID(path.stem)
                    if path.stem == str(block_id):
                        block_ids.add(block_id)
        return sorted(block_ids)

    @staticmethod
    def _read_hash(mount: Path, block_id: UUID) -> str:
        try:
            with (mount / f"{block_id}.sha256").open("rb") as metadata:
                expected_hash = metadata.read(65).decode("ascii")
        except (FileNotFoundError, UnicodeDecodeError) as exc:
            raise CorruptedBlock("Hash persistido ausente ou inválido.") from exc
        if re.fullmatch(SHA256_PATTERN, expected_hash) is None:
            raise CorruptedBlock("Hash persistido inválido.")
        return expected_hash.lower()

    def _verify(self, disk_id: str, block_id: UUID, output: BinaryIO | None = None) -> str:
        mount = self.mount(disk_id)
        digest = hashlib.sha256()
        with self._lock(block_id), (mount / f"{block_id}.blk").open("rb") as block:
            expected_hash = self._read_hash(mount, block_id)
            while chunk := block.read(BUFFER_SIZE):
                digest.update(chunk)
                if output is not None:
                    output.write(chunk)
            if digest.hexdigest() != expected_hash:
                raise CorruptedBlock("O SHA-256 do bloco diverge do hash persistido.")
        return expected_hash

    def verify(self, disk_id: str, block_id: UUID) -> None:
        self._verify(disk_id, block_id)

    def open_verified(self, disk_id: str, block_id: UUID) -> tuple[BinaryIO, int, str]:
        # A cópia validada pode ser transmitida sem manter o bloco original aberto.
        snapshot = tempfile.SpooledTemporaryFile(max_size=BUFFER_SIZE)
        try:
            digest = self._verify(disk_id, block_id, snapshot)
            size = snapshot.tell()
            snapshot.seek(0)
            return snapshot, size, digest
        except BaseException:
            snapshot.close()
            raise

    def _publish(self, disk_id: str, block_id: UUID, temporary: Path, digest: str) -> None:
        mount = self.mount(disk_id)
        block = mount / f"{block_id}.blk"
        hash_file = mount / f"{block_id}.sha256"
        with self._lock(block_id):
            if any(
                path != mount and (path / block.name).is_file()
                for path in self.mounts.values()
            ):
                raise BlockConflict("Este bloco já está armazenado em outro disco do nó.")

            persisted_hash = None
            if block.exists():
                with suppress(CorruptedBlock):
                    persisted_hash = self._read_hash(mount, block_id)
                if persisted_hash is not None and persisted_hash != digest:
                    raise BlockConflict("Este identificador já está associado a outro SHA-256.")

            if persisted_hash is None:
                temporary_hash = temporary.with_suffix(".sha256.tmp")
                try:
                    with temporary_hash.open("xb") as metadata:
                        metadata.write(digest.encode("ascii"))
                        metadata.flush()
                        os.fsync(metadata.fileno())
                    os.replace(temporary_hash, hash_file)
                    self._sync_directory(mount)
                finally:
                    temporary_hash.unlink(missing_ok=True)

            os.replace(temporary, block)
            self._sync_directory(mount)

    @staticmethod
    def _sync_directory(mount: Path) -> None:
        if os.name == "posix":
            fd = os.open(mount, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    async def write(
        self, disk_id: str, block_id: UUID, content: AsyncIterable[bytes], expected_hash: str
    ) -> int:
        mount = self.mount(disk_id)
        digest = hashlib.sha256()
        size = 0
        with tempfile.NamedTemporaryFile(dir=mount, suffix=".blk.tmp", delete=False) as staged:
            temporary = Path(staged.name)
            try:
                async for chunk in content:
                    digest.update(chunk)
                    await to_thread.run_sync(staged.write, chunk)
                    size += len(chunk)
                actual_hash = digest.hexdigest()
                if actual_hash != expected_hash.lower():
                    raise HashMismatch("O SHA-256 do conteúdo recebido diverge do esperado.")
                await to_thread.run_sync(staged.flush)
                await to_thread.run_sync(os.fsync, staged.fileno())
                staged.close()
                await to_thread.run_sync(self._publish, disk_id, block_id, temporary, actual_hash)
            finally:
                staged.close()
                temporary.unlink(missing_ok=True)
        return size

    def delete(self, disk_id: str, block_id: UUID) -> None:
        mount = self.mount(disk_id)
        with self._lock(block_id):
            (mount / f"{block_id}.blk").unlink(missing_ok=True)
            (mount / f"{block_id}.sha256").unlink(missing_ok=True)
            self._sync_directory(mount)

    def verified_inventory(self) -> dict[str, list[UUID]]:
        inventory: dict[str, list[UUID]] = {}
        for disk_id in self.mounts:
            inventory[disk_id] = []
            try:
                block_ids = self.list_blocks(disk_id)
            except OSError as exc:
                logger.warning("disk_inventory_failed", disk_id=disk_id, error=str(exc))
                continue
            for block_id in block_ids:
                try:
                    self.verify(disk_id, block_id)
                except (OSError, CorruptedBlock):
                    continue
                inventory[disk_id].append(block_id)
        return inventory
