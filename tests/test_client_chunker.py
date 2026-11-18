from __future__ import annotations

import hashlib

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.client.chunker import (
    DecryptionError,
    IntegrityError,
    decrypt_block,
    encryption_key,
    prepare_file,
)
from src.common.config import Settings, get_settings


@pytest.mark.parametrize("size", [0, 1, 1024 * 1024, 2 * 1024 * 1024 + 37])
@pytest.mark.parametrize("encrypted", [False, True])
def test_chunk_boundaries_hashes_encryption_and_cleanup(tmp_path, size, encrypted):
    content = (bytes(range(256)) * ((size + 255) // 256))[:size]
    source = tmp_path / "source"
    source.write_bytes(content)
    settings = Settings(
        chunk_size_mb=1, encryption_enabled=encrypted, ENCRYPTION_KEY="ab" * 32
    )
    with prepare_file(source, settings) as prepared:
        spool = prepared.spool
        assert prepared.file_hash == hashlib.sha256(content).hexdigest()
        assert prepared.size_bytes == size
        assert len(prepared.blocks) == (size + 1024 * 1024 - 1) // (1024 * 1024)
        reconstructed = []
        for block in prepared.blocks:
            wire = prepared.read_block(block.block_index)
            assert hashlib.sha256(wire).hexdigest() == block.block_hash
            assert len(wire) == block.size_bytes
            if encrypted:
                assert len(block.encryption_nonce) == 24
                reconstructed.append(
                    decrypt_block(
                        wire, block.encryption_nonce, encryption_key(settings)
                    )
                )
            else:
                assert block.encryption_nonce is None
                reconstructed.append(wire)
        assert b"".join(reconstructed) == content
        assert (
            len({b.encryption_nonce for b in prepared.blocks}) == len(prepared.blocks)
            if encrypted
            else True
        )
    assert not spool.exists()


@pytest.mark.parametrize("key", [None, "", "aa" * 16, "zz" * 32, "a " * 32])
def test_invalid_keys_rejected_without_disclosing_them(key):
    with pytest.raises(ValueError, match="ENCRYPTION_KEY") as error:
        encryption_key(Settings(ENCRYPTION_KEY=key))
    if key:
        assert key not in str(error.value)


def test_wrong_key_and_invalid_nonce_are_authentication_errors():
    nonce = bytes(range(12))
    wire = AESGCM(b"a" * 32).encrypt(nonce, b"secret", None)
    with pytest.raises(DecryptionError):
        decrypt_block(wire, nonce.hex(), b"b" * 32)
    with pytest.raises(DecryptionError):
        decrypt_block(wire, "bad", b"a" * 32)


def test_snapshot_is_stable_and_spool_tampering_detected(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"original")
    with prepare_file(source, Settings()) as prepared:
        source.write_bytes(b"changed")
        assert prepared.read_block(0) == b"original"
        prepared.spool.write_bytes(b"tampered")
        with pytest.raises(IntegrityError):
            prepared.read_block(0)


def test_environment_and_dotenv_keys(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
    (tmp_path / ".env").write_text("ENCRYPTION_KEY=" + "cd" * 32, encoding="utf-8")
    get_settings.cache_clear()
    try:
        assert encryption_key(get_settings()) == bytes.fromhex("cd" * 32)
        monkeypatch.setenv("ENCRYPTION_KEY", "ef" * 32)
        get_settings.cache_clear()
        assert encryption_key(get_settings()) == bytes.fromhex("ef" * 32)
    finally:
        get_settings.cache_clear()
