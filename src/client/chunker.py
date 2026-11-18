"""Preparação em disco com memória limitada a um bloco e cifras exclusivas do cliente."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.common.config import Settings
from src.common.schemas import BlockAllocateItem


class IntegrityError(ValueError):
    """Conteúdo ou metadados não passaram pela verificação de integridade."""


class DecryptionError(ValueError):
    """Falha de autenticação: chave incorreta ou metadados cifrados inválidos."""


def encryption_key(settings: Settings) -> bytes:
    if settings.encryption_algorithm.upper() != "AES-256-GCM":
        raise ValueError("O cliente suporta somente AES-256-GCM.")
    value = settings.encryption_key or ""
    if re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError(
            "Defina ENCRYPTION_KEY com exatamente 64 caracteres hexadecimais."
        )
    return bytes.fromhex(value)


def decrypt_block(content: bytes, nonce: str, key: bytes) -> bytes:
    if re.fullmatch(r"[0-9a-fA-F]{24}", nonce) is None:
        raise DecryptionError("Nonce AES-GCM inválido nos metadados.")
    try:
        return AESGCM(key).decrypt(bytes.fromhex(nonce), content, None)
    except InvalidTag as exc:
        # Um SHA-256 válido com tag inválida não identifica uma réplica corrompida.
        raise DecryptionError(
            "Falha ao autenticar o bloco AES-GCM; verifique ENCRYPTION_KEY."
        ) from exc


@dataclass(frozen=True)
class PreparedFile:
    spool: Path
    size_bytes: int
    file_hash: str
    is_encrypted: bool
    blocks: list[BlockAllocateItem]
    offsets: list[int]

    def read_block(self, index: int) -> bytes:
        item = self.blocks[index]
        with self.spool.open("rb") as source:
            source.seek(self.offsets[index])
            content = source.read(item.size_bytes)
        if (
            len(content) != item.size_bytes
            or hashlib.sha256(content).hexdigest() != item.block_hash
        ):
            raise IntegrityError("O bloco temporário mudou após sua preparação.")
        return content


@contextmanager
def prepare_file(
    source: str | Path,
    settings: Settings,
    *,
    encrypt: bool | None = None,
) -> Iterator[PreparedFile]:
    """Calcula SHA-256 original e dos bytes de rede em uma única leitura da origem.

    O snapshot temporário evita que alterações na origem entre allocate e PUT
    invalidem os hashes. Nonces são gerados uma única vez e reutilizados nas réplicas.
    """
    if settings.hash_algorithm.lower() != "sha256":
        raise ValueError("O cliente suporta somente SHA-256.")
    enabled = settings.encryption_enabled if encrypt is None else encrypt
    cipher = AESGCM(encryption_key(settings)) if enabled else None
    blocks, offsets = [], []
    file_digest = hashlib.sha256()
    original_size = 0
    with tempfile.TemporaryDirectory(prefix="dcomp-client-") as directory:
        spool = Path(directory) / "blocks.bin"
        with Path(source).open("rb") as incoming, spool.open("wb") as outgoing:
            while content := incoming.read(settings.chunk_size_mb * 1024 * 1024):
                file_digest.update(content)
                original_size += len(content)
                nonce = os.urandom(12) if cipher else None
                wire = cipher.encrypt(nonce, content, None) if cipher else content
                offsets.append(outgoing.tell())
                outgoing.write(wire)
                blocks.append(
                    BlockAllocateItem(
                        block_index=len(blocks),
                        size_bytes=len(wire),
                        block_hash=hashlib.sha256(wire).hexdigest(),
                        encryption_nonce=nonce.hex() if nonce else None,
                    )
                )
        yield PreparedFile(
            spool, original_size, file_digest.hexdigest(), enabled, blocks, offsets
        )
