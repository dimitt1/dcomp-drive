"""Logger estruturado em JSON para atender ao RF12 (Registro de eventos)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class StructuredJsonFormatter(logging.Formatter):
    """Formata registros de log como JSON estruturado em uma linha."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "event": getattr(record, "event", record.getMessage()),
        }
        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            for k, v in extra_fields.items():
                if v is not None:
                    payload[k] = str(v) if not isinstance(v, (int, float, bool, list, dict)) else v

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


class ComponentLogger:
    """Wrapper para emissão de logs estruturados por componente (RF12)."""

    def __init__(self, component: str) -> None:
        self.component = component
        self._logger = logging.getLogger(f"dcomp_drive.{component}")
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(StructuredJsonFormatter())
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
            self._logger.propagate = False

    def info(self, event: str, **kwargs: Any) -> None:
        self._logger.info(
            event,
            extra={"component": self.component, "event": event, "extra_fields": kwargs},
        )

    def warning(self, event: str, **kwargs: Any) -> None:
        self._logger.warning(
            event,
            extra={"component": self.component, "event": event, "extra_fields": kwargs},
        )

    def error(self, event: str, **kwargs: Any) -> None:
        self._logger.error(
            event,
            extra={"component": self.component, "event": event, "extra_fields": kwargs},
        )


def get_logger(component: str) -> ComponentLogger:
    """Retorna um logger estruturado para o componente informado."""
    return ComponentLogger(component)
