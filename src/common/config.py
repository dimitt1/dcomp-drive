"""Carregador de configurações do sistema via config.yaml e variáveis de ambiente."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configurações globais do DComp Drive."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Parâmetros obrigatórios (RF3, RF9)
    encryption_enabled: bool = Field(default=False, description="Habilita cifra AES-256-GCM no cliente")
    encryption_algorithm: str = Field(default="AES-256-GCM")
    encryption_key: str | None = Field(
        default=None,
        validation_alias="ENCRYPTION_KEY",
        description="Chave simétrica de 256 bits (64 hex chars) exclusiva do Cliente via variável de ambiente/.env",
    )
    replication_factor: int = Field(default=3, ge=1, description="Nível de replicação padrão (N)")

    # Parâmetros recomendados e opcionais
    chunk_size_mb: int = Field(default=4, ge=1)
    write_quorum: int = Field(default=2, ge=1, description="Quórum de escrita (W)")
    allow_under_replicated_writes: bool = Field(
        default=True,
        description="Se true, aceita gravações sub-replicadas quando há menos máquinas que N",
    )
    hash_algorithm: str = Field(default="sha256")

    # Heartbeats e detecção de falhas (RF5)
    heartbeat_interval_sec: int = Field(default=5, ge=1)
    suspect_timeout_sec: int = Field(default=10, ge=1)
    heartbeat_timeout_sec: int = Field(default=15, ge=1)

    # Scrubbing e Concorrência (RF4, RF8)
    scrubbing_interval_sec: int = Field(default=60, ge=1)
    lease_timeout_sec: int = Field(default=300, ge=5, description="Tempo de expiração do lease de escrita")
    lease_cleanup_interval_sec: int = Field(default=15, ge=1)

    # Espaço e Re-replicação (RF5, RF7)
    disk_usage_limit_pct: float = Field(default=90.0, gt=0.0, le=100.0)
    placement_policy: str = Field(default="free_space")
    max_concurrent_replications: int = Field(default=4, ge=1)
    monitor_interval_sec: int = Field(default=5, ge=1)

    # Conexões
    database_url: str = Field(
        default="postgresql+asyncpg://dcomp:dcomp@localhost:5433/dcomp_drive",
        validation_alias="DATABASE_URL",
    )
    controller_url: str = Field(
        default="http://localhost:8000",
        validation_alias="CONTROLLER_URL",
    )


def load_yaml_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Lê o arquivo config.yaml se existir."""
    path = Path(
        config_path
        or os.getenv("CONFIG_PATH")
        or Path(__file__).resolve().parents[2] / "config.yaml"
    )
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                return data
    return {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna a instância de configurações mesclando config.yaml e variáveis de ambiente."""
    yaml_data = load_yaml_config()
    # Variáveis de ambiente têm precedência sobre o config.yaml
    for field_name in Settings.model_fields:
        env_key = field_name.upper()
        if env_key in os.environ:
            yaml_data[field_name] = os.environ[env_key]
    return Settings(**yaml_data)
