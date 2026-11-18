"""API assíncrona do cliente: controle JSON e blocos diretamente nos nós."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ValidationError

from src.client.chunker import (
    IntegrityError,
    decrypt_block,
    encryption_key,
    prepare_file,
)
from src.common.config import Settings, get_settings
from src.common.schemas import (
    ConfirmedReplicaItem,
    DirectoryCreateRequest,
    DirectoryListResponse,
    EntryDeleteResponse,
    EntryMoveRequest,
    FileAllocateRequest,
    FileAllocateResponse,
    FileCommitRequest,
    FileCommitResponse,
    FileMetadataResponse,
    NodeStatus,
    ReplicaStatus,
    ReportCorruptionRequest,
)


class ClientError(Exception):
    """Erro de operação remoto, protocolo ou indisponibilidade."""


async def _local(operation, *args):
    """Espera E/S local terminar antes de liberar/remover arquivos temporários."""
    task = asyncio.create_task(asyncio.to_thread(operation, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class DriveClient:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        controller_url: str | None = None,
        concurrency: int = 4,
        timeout: float = 30.0,
        http: httpx.AsyncClient | None = None,
        node_urls: dict[str, str] | None = None,
    ) -> None:
        if concurrency < 1 or timeout <= 0:
            raise ValueError("Concorrência e timeout devem ser positivos.")
        self.settings = settings or get_settings()
        self.controller_url = (controller_url or self.settings.controller_url).rstrip(
            "/"
        )
        self._validate_url(self.controller_url)
        self.node_urls = node_urls or {}
        for url in self.node_urls.values():
            self._validate_url(url)
        self.concurrency = concurrency
        self.timeout = timeout
        self.http = http or httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(
                max_connections=concurrency, max_keepalive_connections=concurrency
            ),
        )
        self._owns_http = http is None
        self.warnings: list[str] = []
        self._network = asyncio.Semaphore(concurrency)

    @staticmethod
    def _validate_url(url: str) -> None:
        parsed = httpx.URL(url)
        if parsed.scheme not in ("http", "https") or not parsed.host or parsed.userinfo:
            raise ValueError("URLs devem usar HTTP(S), sem credenciais embutidas.")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_http:
            await self.http.aclose()

    def _node_url(self, replica: Any, block_id: Any) -> str:
        host = self.node_urls.get(replica.node_id, replica.host_url).rstrip("/")
        self._validate_url(host)
        return f"{host}/blocks/{block_id}"

    async def _control(
        self, method: str, path: str, model: type[BaseModel] | None = None, **kwargs
    ):
        try:
            response = await self.http.request(
                method,
                f"{self.controller_url}{path}",
                timeout=self.timeout,
                **kwargs,
            )
            if not response.is_success:
                try:
                    detail = response.json().get("detail", "Requisição rejeitada.")
                except (ValueError, AttributeError):
                    detail = "Resposta de erro sem detalhe JSON."
                raise ClientError(f"Controlador HTTP {response.status_code}: {detail}")
            value = response.json()
            return model.model_validate(value) if model else value
        except httpx.HTTPError as exc:
            raise ClientError(
                f"Falha de comunicação com o controlador ({type(exc).__name__})."
            ) from exc
        except (ValueError, ValidationError) as exc:
            raise ClientError("Resposta inválida do controlador.") from exc

    async def mkdir(self, path: str, *, parents: bool = True) -> FileMetadataResponse:
        return await self._control(
            "POST",
            "/dirs",
            FileMetadataResponse,
            json=DirectoryCreateRequest(path=path, parents=parents).model_dump(
                mode="json"
            ),
        )

    async def ls(self, path: str = "/") -> DirectoryListResponse:
        return await self._control(
            "GET", "/dirs", DirectoryListResponse, params={"path": path}
        )

    async def stat(self, path: str) -> FileMetadataResponse:
        return await self._control(
            "GET",
            "/files/metadata",
            FileMetadataResponse,
            params={"path": path, "include_all_replicas": True},
        )

    async def rm(self, path: str, *, recursive: bool = False) -> EntryDeleteResponse:
        return await self._control(
            "DELETE",
            "/entries",
            EntryDeleteResponse,
            params={"path": path, "recursive": recursive},
        )

    async def mv(self, source: str, destination: str) -> FileMetadataResponse:
        return await self._control(
            "PATCH",
            "/entries/move",
            FileMetadataResponse,
            json=EntryMoveRequest(
                source_path=source, destination_path=destination
            ).model_dump(mode="json"),
        )

    async def cluster_status(self) -> dict:
        result = await self._control("GET", "/cluster/status")
        if not isinstance(result, dict) or not isinstance(result.get("nodes"), list):
            raise ClientError("Resposta inválida de estado do cluster.")
        return result

    async def drain(self, node_id: str) -> dict:
        return await self._control("POST", f"/nodes/{quote(node_id, safe='')}/drain")

    async def _parallel(self, items, operation):
        """Número limitado de tarefas, inclusive para arquivos com muitos blocos."""
        iterator = iter(items)

        async def worker():
            results = []
            for item in iterator:
                results.append(await operation(item))
            return results

        tasks = [
            asyncio.create_task(worker())
            for _ in range(min(self.concurrency, len(items)))
        ]
        try:
            return [
                result for group in await asyncio.gather(*tasks) for result in group
            ]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def put(
        self,
        source: str | Path,
        destination: str,
        *,
        encrypt: bool | None = None,
        replication_target: int | None = None,
    ) -> FileCommitResponse:
        # Toda a preparação antecede o lease; arquivo grande não consome o lease em hashing.
        with prepare_file(source, self.settings, encrypt=encrypt) as prepared:
            request = FileAllocateRequest(
                path=destination,
                size_bytes=prepared.size_bytes,
                file_hash=prepared.file_hash,
                is_encrypted=prepared.is_encrypted,
                replication_target=replication_target,
                blocks=prepared.blocks,
            )
            allocation = await self._control(
                "POST",
                "/files/allocate",
                FileAllocateResponse,
                json=request.model_dump(mode="json"),
            )
            if allocation.under_replicated_warning:
                self.warnings.append(allocation.under_replicated_warning)
            blocks = sorted(allocation.blocks, key=lambda b: b.block_index)
            if len(blocks) != len(prepared.blocks) or allocation.write_quorum < 1:
                raise ClientError("Plano de alocação inválido.")
            minimum_targets = min((len(block.targets) for block in blocks), default=0)
            for expected, actual in zip(prepared.blocks, blocks):
                if any(
                    getattr(expected, name) != getattr(actual, name)
                    for name in (
                        "block_index",
                        "size_bytes",
                        "block_hash",
                        "encryption_nonce",
                    )
                ):
                    raise ClientError("O controlador alterou os metadados dos blocos.")
                if any(
                    len({getattr(t, field) for t in actual.targets})
                    != len(actual.targets)
                    for field in ("node_id", "machine_id", "disk_id")
                ):
                    raise ClientError("Alocação contém destinos repetidos.")

            async def upload(block):
                wire = await _local(prepared.read_block, block.block_index)

                async def replica(target):
                    try:
                        async with self._network:
                            response = await self.http.put(
                                self._node_url(target, block.block_id),
                                params={
                                    "disk_id": target.disk_id,
                                    "expected_hash": block.block_hash,
                                },
                                content=wire,
                                timeout=self.timeout,
                                headers={
                                    "Content-Type": "application/octet-stream",
                                    "Content-Length": str(len(wire)),
                                },
                            )
                        response.raise_for_status()
                        ack = response.json()
                        if (
                            ack.get("block_id") != str(block.block_id)
                            or ack.get("disk_id") != target.disk_id
                            or ack.get("size_bytes") != len(wire)
                        ):
                            raise ValueError("Confirmação de bloco inválida.")
                        return ConfirmedReplicaItem(
                            block_id=block.block_id,
                            node_id=target.node_id,
                            disk_id=target.disk_id,
                        )
                    except (httpx.HTTPError, ValueError, AttributeError):
                        self.warnings.append(
                            f"Réplica não confirmada: bloco {block.block_index}, nó {target.node_id}."
                        )
                        return None

                # A API anuncia o menor W entre blocos. Quando o número de
                # destinos varia, confirmar todos nos blocos maiores também
                # satisfaz o W por bloco exigido pelo controlador no commit.
                required = (
                    len(block.targets)
                    if len(block.targets) > minimum_targets
                    else allocation.write_quorum
                )
                if len(block.targets) < required:
                    raise ClientError("Plano sem destinos suficientes para o quórum.")
                pending = {asyncio.create_task(replica(t)) for t in block.targets}
                tasks = set(pending)
                confirmed = []
                try:
                    while pending and len(confirmed) < required:
                        done, pending = await asyncio.wait(
                            pending, return_when=asyncio.FIRST_COMPLETED
                        )
                        confirmed.extend(
                            result
                            for task in done
                            if (result := task.result()) is not None
                        )
                    if len(confirmed) < required:
                        raise ClientError(
                            f"Quórum não atingido no bloco {block.block_index}: {len(confirmed)}/{required}. O lease será limpo pelo controlador."
                        )
                    return confirmed
                finally:
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

            groups = await self._parallel(blocks, upload)
            commit = FileCommitRequest(
                entry_id=allocation.entry_id,
                confirmed_replicas=[replica for group in groups for replica in group],
            )
            result = await self._control(
                "POST",
                "/files/commit",
                FileCommitResponse,
                json=commit.model_dump(mode="json"),
            )
            if result.sub_replicated_blocks:
                self.warnings.append(result.message)
            return result

    async def _report(self, block, replica) -> None:
        payload = ReportCorruptionRequest(
            block_id=block.block_id,
            node_id=replica.node_id,
            disk_id=replica.disk_id,
            reporter="client",
        )
        try:
            await self._control(
                "POST",
                "/blocks/report-corruption",
                json=payload.model_dump(mode="json"),
            )
        except ClientError:
            # O próprio nó pode já ter reportado e removido esta réplica (404).
            self.warnings.append(
                f"Não foi possível confirmar o reporte de corrupção: {block.block_id}, {replica.node_id}."
            )

    async def _download_block(self, block) -> bytes:
        replicas = sorted(
            (
                r
                for r in block.replicas
                if r.status == ReplicaStatus.VERIFIED
                and r.node_status in (NodeStatus.ALIVE, NodeStatus.DRAINING)
            ),
            key=lambda r: (r.node_status != NodeStatus.ALIVE, r.node_id),
        )
        for replica in replicas:
            try:
                async with (
                    self._network,
                    self.http.stream(
                        "GET",
                        self._node_url(replica, block.block_id),
                        params={"disk_id": replica.disk_id},
                        timeout=self.timeout,
                        headers={"Accept-Encoding": "identity"},
                    ) as response,
                ):
                    if response.status_code == 422:
                        raise IntegrityError("O nó detectou corrupção local.")
                    response.raise_for_status()
                    wire = bytearray()
                    digest = hashlib.sha256()
                    async for piece in response.aiter_bytes(chunk_size=64 * 1024):
                        if len(wire) + len(piece) > block.size_bytes:
                            raise IntegrityError("Bloco maior que o tamanho declarado.")
                        wire.extend(piece)
                        digest.update(piece)
                    if (
                        len(wire) != block.size_bytes
                        or digest.hexdigest() != block.block_hash.lower()
                    ):
                        raise IntegrityError("SHA-256 ou tamanho do bloco divergente.")
                return bytes(wire)
            except IntegrityError:
                await self._report(block, replica)
                self.warnings.append(
                    f"Réplica corrompida descartada: {block.block_id}, {replica.node_id}."
                )
            except httpx.HTTPError:
                self.warnings.append(
                    f"Réplica indisponível: {block.block_id}, {replica.node_id}."
                )
        raise ClientError(
            f"Nenhuma réplica íntegra disponível para o bloco {block.block_index} ({block.block_id})."
        )

    async def get(self, source: str, destination: str | Path) -> FileMetadataResponse:
        metadata = await self._control(
            "GET",
            "/files/metadata",
            FileMetadataResponse,
            params={"path": source, "for_read": True},
        )
        if metadata.is_dir:
            raise ClientError("get exige um arquivo, não um diretório.")
        key = encryption_key(self.settings) if metadata.is_encrypted else None
        blocks = sorted(metadata.blocks, key=lambda b: b.block_index)
        if [b.block_index for b in blocks] != list(range(len(blocks))):
            raise IntegrityError("Índices de blocos não contíguos.")
        for block in blocks:
            if re.fullmatch(r"[0-9a-fA-F]{64}", block.block_hash) is None:
                raise IntegrityError("Hash SHA-256 de bloco inválido.")
            if (
                metadata.is_encrypted
                and re.fullmatch(r"[0-9a-fA-F]{24}", block.encryption_nonce or "")
                is None
            ):
                raise IntegrityError("Nonce AES-GCM inválido nos metadados.")
            if not metadata.is_encrypted and block.encryption_nonce is not None:
                raise IntegrityError("Arquivo não cifrado contém nonce inesperado.")
        if (
            not metadata.file_hash
            or re.fullmatch(r"[0-9a-fA-F]{64}", metadata.file_hash) is None
        ):
            raise IntegrityError("Hash SHA-256 do arquivo ausente ou inválido.")
        lengths = [b.size_bytes - (16 if metadata.is_encrypted else 0) for b in blocks]
        if any(size < 0 for size in lengths) or sum(lengths) != metadata.size_bytes:
            raise IntegrityError("Tamanho dos blocos incompatível com o arquivo.")
        offsets, position = [], 0
        for size in lengths:
            offsets.append(position)
            position += size
        destination = Path(destination)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".part",
                delete=False,
            ) as output:
                temporary = Path(output.name)
                output.truncate(metadata.size_bytes)

            async def download(block):
                wire = await self._download_block(block)
                content = (
                    decrypt_block(wire, block.encryption_nonce or "", key)
                    if key is not None
                    else wire
                )

                def write():
                    with temporary.open("r+b") as output:
                        output.seek(offsets[block.block_index])
                        output.write(content)

                await _local(write)

            await self._parallel(blocks, download)

            def verify():
                digest = hashlib.sha256()
                with temporary.open("r+b") as output:
                    while piece := output.read(1024 * 1024):
                        digest.update(piece)
                    if digest.hexdigest() != metadata.file_hash.lower():
                        raise IntegrityError(
                            "O SHA-256 do arquivo reconstruído diverge dos metadados."
                        )
                    output.flush()
                    os.fsync(output.fileno())

            await _local(verify)
            os.replace(temporary, destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return metadata
