"""Interface Web estilo Google Drive e Painel do Plano de Controle do DComp Drive."""

from __future__ import annotations

import os
import posixpath
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.background import BackgroundTask

from src.client.chunker import DecryptionError, IntegrityError
from src.client.transfer import ClientError, DriveClient
from src.common.config import Settings, get_settings
from src.common.logger import get_logger
from src.common.schemas import EntryStatus, EntrySummaryResponse
from src.controller.database import get_db
from src.controller.models import Block, BlockReplica, Entry, utc_now
from src.controller.monitor import run_cluster_monitor_cycle
from src.controller.routes_metadata import (
    _ensure_aware,
    evaluate_entry_health,
    get_entry_name,
)

logger = get_logger("controller.web_ui")

router = APIRouter(tags=["Web UI (Google Drive)"])

DEFAULT_UI_DEMO_KEY = "ab" * 32


def _build_ui_settings(request: Request) -> Settings:
    """Constrói Settings para o cliente interno da UI apontando para a URL local do controlador."""
    base_cfg = get_settings()
    base_url = str(request.base_url).rstrip("/")
    data = base_cfg.model_dump()
    data["controller_url"] = base_url
    if not data.get("encryption_key"):
        data["encryption_key"] = os.getenv("ENCRYPTION_KEY", DEFAULT_UI_DEMO_KEY)
    return Settings(**data)


@router.get("/ui/api/entries")
async def ui_list_all_entries(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Lista todas as entradas (arquivos e pastas) do DComp Drive para busca global e filtros da UI."""
    stmt = (
        select(Entry)
        .options(
            selectinload(Entry.blocks)
            .selectinload(Block.replicas)
            .selectinload(BlockReplica.node)
        )
        .where(Entry.path != "/")
        .order_by(Entry.is_dir.desc(), Entry.path.asc())
    )
    entries = (await db.execute(stmt)).scalars().all()

    items: list[EntrySummaryResponse] = []
    status_changed = False
    total_logical_bytes = 0

    for item in entries:
        if not item.is_dir:
            total_logical_bytes += item.size_bytes or 0
            if item.status != EntryStatus.WRITING.value:
                computed = evaluate_entry_health(item)
                if computed.value != item.status:
                    item.status = computed.value
                    status_changed = True

        items.append(
            EntrySummaryResponse(
                entry_id=item.entry_id,
                path=item.path,
                name=get_entry_name(item.path),
                parent_path=item.parent_path,
                is_dir=item.is_dir,
                size_bytes=item.size_bytes,
                file_hash=item.file_hash,
                replication_target=item.replication_target,
                is_encrypted=item.is_encrypted,
                status=EntryStatus(item.status),
                created_at=_ensure_aware(item.created_at) or utc_now(),
                updated_at=_ensure_aware(item.updated_at) or utc_now(),
            )
        )

    if status_changed:
        await db.commit()

    return {
        "total_items": len(items),
        "total_logical_bytes": total_logical_bytes,
        "entries": [i.model_dump(mode="json") for i in items],
    }


@router.post("/ui/api/upload", status_code=status.HTTP_201_CREATED)
async def ui_upload_file(
    request: Request,
    path: str = Query(..., description="Caminho de destino no DComp Drive (ex: /docs/arquivo.txt)"),
    encrypt: bool = Query(default=False, description="Cifrar blocos com AES-256-GCM"),
    replication_target: int = Query(default=3, ge=1, le=10, description="Fator de replicação N"),
) -> dict[str, Any]:
    """Recebe upload binário da interface web e executa o fluxo completo de escrita no cluster."""
    ui_settings = _build_ui_settings(request)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="dcomp-ui-upload-", delete=False) as tmp:
            temp_path = Path(tmp.name)
            async for chunk in request.stream():
                tmp.write(chunk)

        async with DriveClient(
            settings=ui_settings,
            controller_url=ui_settings.controller_url,
            timeout=30.0,
        ) as client:
            commit_resp = await client.put(
                temp_path,
                path,
                encrypt=encrypt,
                replication_target=replication_target,
            )
            payload = commit_resp.model_dump(mode="json")
            payload["warnings"] = list(client.warnings)
            return payload
    except (ClientError, IntegrityError, DecryptionError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@router.get("/ui/api/download")
async def ui_download_file(
    request: Request,
    path: str = Query(..., description="Caminho do arquivo no DComp Drive"),
) -> FileResponse:
    """Baixa e valida todos os blocos de um arquivo dos Storage Nodes e entrega ao navegador."""
    ui_settings = _build_ui_settings(request)
    filename = posixpath.basename(path.strip("/")) or "download.bin"
    fd, raw_tmp = tempfile.mkstemp(prefix="dcomp-ui-dl-", suffix=f"-{filename}")
    os.close(fd)
    temp_path = Path(raw_tmp)

    try:
        async with DriveClient(
            settings=ui_settings,
            controller_url=ui_settings.controller_url,
            timeout=30.0,
        ) as client:
            await client.get(path, temp_path)
    except (ClientError, IntegrityError, DecryptionError, ValueError) as exc:
        temp_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return FileResponse(
        path=temp_path,
        filename=filename,
        media_type="application/octet-stream",
        background=BackgroundTask(lambda: temp_path.unlink(missing_ok=True)),
    )


@router.post("/ui/api/monitor-cycle")
async def ui_trigger_monitor_cycle() -> dict[str, Any]:
    """Dispara manualmente um ciclo de detecção de falhas e fila de re-replicação."""
    return await run_cluster_monitor_cycle()


DRIVE_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>DComp Drive — Armazenamento Distribuído</title>
  <style>
    :root {
      --bg-shell: #f8fafd;
      --bg-surface: #ffffff;
      --bg-hover: #f1f3f4;
      --bg-selected: #c2e7ff;
      --bg-selected-soft: #e8f0fe;
      --bg-search: #e9eef6;
      --text-primary: #1f1f1f;
      --text-secondary: #444746;
      --text-muted: #747775;
      --border-color: #e0e3e7;
      --border-subtle: #edf2fc;
      --brand-blue: #0b57d0;
      --brand-blue-hover: #0842a0;
      --status-healthy-bg: #e6f4ea;
      --status-healthy-fg: #137333;
      --status-warn-bg: #fef7e0;
      --status-warn-fg: #b06000;
      --status-recover-bg: #e8f0fe;
      --status-recover-fg: #1967d2;
      --status-danger-bg: #fce8e6;
      --status-danger-fg: #c5221f;
      --status-drain-bg: #f3e8fd;
      --status-drain-fg: #7627bb;
      --shadow-fab: 0 1px 3px 0 rgba(60,64,67,0.3), 0 4px 8px 3px rgba(60,64,67,0.15);
      --shadow-card: 0 1px 2px 0 rgba(60,64,67,0.15), 0 1px 3px 1px rgba(60,64,67,0.08);
      --shadow-modal: 0 12px 36px rgba(0,0,0,0.22);
      --radius-lg: 16px;
      --radius-md: 12px;
      --radius-pill: 999px;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: "Google Sans", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background-color: var(--bg-shell);
      color: var(--text-primary);
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      user-select: none;
    }

    /* Top Header (Google Workspace Bar) */
    .topbar {
      height: 64px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 20px 8px 16px;
      gap: 16px;
      flex-shrink: 0;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 228px;
      cursor: pointer;
      flex-shrink: 0;
    }

    .brand-logo {
      width: 38px;
      height: 38px;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .brand-title {
      font-size: 21px;
      font-weight: 500;
      color: var(--text-secondary);
      letter-spacing: -0.3px;
    }

    .brand-subtitle {
      font-size: 11px;
      font-weight: 600;
      color: var(--brand-blue);
      background: var(--bg-selected-soft);
      padding: 2px 7px;
      border-radius: 6px;
      margin-left: 4px;
    }

    .search-wrapper {
      flex: 1;
      max-width: 680px;
      display: flex;
      align-items: center;
      background: var(--bg-search);
      border-radius: var(--radius-pill);
      padding: 0 16px;
      height: 46px;
      transition: background 0.15s, box-shadow 0.15s;
    }

    .search-wrapper:focus-within {
      background: #ffffff;
      box-shadow: 0 1px 3px rgba(60,64,67,0.28);
    }

    .search-wrapper svg {
      color: var(--text-secondary);
      flex-shrink: 0;
    }

    .search-input {
      border: none;
      background: transparent;
      outline: none;
      width: 100%;
      padding: 0 12px;
      font-size: 14.5px;
      color: var(--text-primary);
    }

    .topbar-right {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
    }

    .cluster-pill {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 6px 12px;
      border-radius: var(--radius-pill);
      font-size: 12.5px;
      font-weight: 500;
      background: #ffffff;
      border: 1px solid var(--border-color);
      color: var(--text-secondary);
      cursor: pointer;
      white-space: nowrap;
      transition: background 0.15s;
    }

    .cluster-pill:hover {
      background: var(--bg-hover);
    }

    .dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #188038;
      flex-shrink: 0;
    }

    .dot.warn { background: #f29900; }
    .dot.danger { background: #d93025; }

    .icon-btn {
      width: 36px;
      height: 36px;
      border-radius: 50%;
      border: none;
      background: transparent;
      color: var(--text-secondary);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      transition: background 0.15s;
      flex-shrink: 0;
    }

    .icon-btn:hover {
      background: rgba(68, 71, 70, 0.08);
    }

    .icon-btn.active {
      background: var(--bg-selected);
      color: #001d35;
    }

    /* Main Workspace Layout */
    .workspace {
      flex: 1;
      display: flex;
      overflow: hidden;
      padding: 0 16px 16px 0;
    }

    /* Left Sidebar */
    .sidebar {
      width: 248px;
      padding: 4px 12px 12px 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
      flex-shrink: 0;
    }

    .btn-new {
      display: inline-flex;
      align-items: center;
      gap: 12px;
      height: 54px;
      padding: 0 22px 0 16px;
      background: #ffffff;
      color: var(--text-primary);
      border: none;
      border-radius: 16px;
      box-shadow: var(--shadow-fab);
      font-size: 14.5px;
      font-weight: 500;
      cursor: pointer;
      width: fit-content;
      transition: box-shadow 0.2s, background 0.15s;
    }

    .btn-new:hover {
      background: #f8fafd;
      box-shadow: 0 2px 6px 1px rgba(60,64,67,0.3), 0 6px 12px 4px rgba(60,64,67,0.15);
    }

    .nav-list {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .nav-item {
      display: flex;
      align-items: center;
      gap: 12px;
      height: 38px;
      padding: 0 14px;
      border-radius: var(--radius-pill);
      border: none;
      background: transparent;
      color: var(--text-secondary);
      font-size: 13.5px;
      font-weight: 500;
      cursor: pointer;
      text-align: left;
      width: 100%;
      white-space: nowrap;
      transition: background 0.12s;
    }

    .nav-item svg {
      flex-shrink: 0;
    }

    .nav-item:hover {
      background: rgba(68, 71, 70, 0.08);
    }

    .nav-item.active {
      background: var(--bg-selected);
      color: #001d35;
      font-weight: 600;
    }

    .nav-badge {
      margin-left: auto;
      font-size: 10.5px;
      padding: 1px 6px;
      border-radius: 99px;
      background: rgba(11, 87, 208, 0.12);
      color: var(--brand-blue);
      font-weight: 600;
    }

    .storage-box {
      margin-top: auto;
      padding: 14px;
      background: #ffffff;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      display: flex;
      flex-direction: column;
      gap: 7px;
    }

    .storage-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 12.5px;
      font-weight: 600;
      color: var(--text-secondary);
    }

    .progress-track {
      width: 100%;
      height: 6px;
      background: #e0e3e7;
      border-radius: 3px;
      overflow: hidden;
    }

    .progress-fill {
      height: 100%;
      background: var(--brand-blue);
      border-radius: 3px;
      transition: width 0.3s ease;
    }

    .progress-fill.warn { background: #f29900; }
    .progress-fill.danger { background: #d93025; }

    .storage-caption {
      font-size: 11.5px;
      color: var(--text-muted);
      line-height: 1.35;
    }

    .storage-meta-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      margin-top: 2px;
    }

    .mini-tag {
      font-size: 10.5px;
      padding: 2px 6px;
      border-radius: 6px;
      background: var(--bg-shell);
      color: var(--text-secondary);
      border: 1px solid var(--border-color);
      font-weight: 500;
    }

    /* Main Drive Canvas */
    .canvas-container {
      flex: 1;
      background: var(--bg-surface);
      border-radius: var(--radius-lg);
      display: flex;
      overflow: hidden;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
      border: 1px solid var(--border-color);
      position: relative;
      min-width: 0;
    }

    .main-view {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      min-width: 0;
    }

    /* Drive Subheader: Breadcrumbs & Toolbar */
    .drive-header {
      padding: 12px 20px 10px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      border-bottom: 1px solid var(--border-subtle);
      flex-wrap: wrap;
    }

    .breadcrumbs {
      display: flex;
      align-items: center;
      gap: 2px;
      flex-wrap: wrap;
      min-width: 0;
    }

    .crumb {
      font-size: 17px;
      font-weight: 500;
      color: var(--text-secondary);
      padding: 5px 9px;
      border-radius: 8px;
      border: none;
      background: transparent;
      cursor: pointer;
      transition: background 0.12s;
    }

    .crumb:hover {
      background: var(--bg-hover);
      color: var(--text-primary);
    }

    .crumb.current {
      color: var(--text-primary);
      font-weight: 600;
    }

    .crumb-sep {
      color: var(--text-muted);
      font-size: 15px;
      user-select: none;
    }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      height: 34px;
      padding: 0 13px;
      border-radius: var(--radius-pill);
      font-size: 13px;
      font-weight: 500;
      cursor: pointer;
      border: 1px solid var(--border-color);
      background: #ffffff;
      color: var(--text-secondary);
      white-space: nowrap;
      transition: background 0.15s, border-color 0.15s;
    }

    .btn:hover {
      background: var(--bg-hover);
    }

    .btn-primary {
      background: var(--brand-blue);
      color: #ffffff;
      border-color: var(--brand-blue);
    }

    .btn-primary:hover {
      background: var(--brand-blue-hover);
    }

    .btn-tonal {
      background: var(--bg-selected-soft);
      color: var(--brand-blue);
      border-color: transparent;
    }

    .btn-tonal:hover {
      background: #d2e3fc;
    }

    .view-toggle {
      display: inline-flex;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-pill);
      overflow: hidden;
    }

    .view-toggle button {
      border: none;
      background: #ffffff;
      padding: 5px 11px;
      cursor: pointer;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      gap: 4px;
      font-size: 12px;
    }

    .view-toggle button.active {
      background: var(--bg-selected);
      color: #001d35;
      font-weight: 600;
    }

    /* Filter Chips Bar (wraps cleanly without ugly horizontal scrollbar) */
    .chips-bar {
      padding: 8px 20px;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px;
      border-bottom: 1px solid var(--border-color);
      flex-shrink: 0;
    }

    .chip {
      padding: 4px 11px;
      border-radius: 8px;
      border: 1px solid var(--border-color);
      background: #ffffff;
      color: var(--text-secondary);
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      white-space: nowrap;
      transition: all 0.12s;
    }

    .chip:hover {
      background: var(--bg-hover);
    }

    .chip.active {
      background: var(--bg-selected);
      border-color: transparent;
      color: #001d35;
      font-weight: 600;
    }

    /* Scrollable Content Area */
    .content-scroll {
      flex: 1;
      overflow-y: auto;
      padding: 18px 20px 32px;
    }

    .section-label {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-secondary);
      margin-bottom: 10px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    /* Folders Grid (Google Drive style pills) */
    .folders-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
      gap: 10px;
      margin-bottom: 22px;
    }

    .folder-card {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 12px;
      background: var(--bg-shell);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      cursor: pointer;
      transition: background 0.15s, box-shadow 0.15s, border-color 0.15s;
    }

    .folder-card:hover {
      background: #edf2fc;
      border-color: #c2e7ff;
    }

    .folder-card.selected {
      background: var(--bg-selected);
      border-color: #7fcfff;
    }

    .folder-card-left {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }

    .folder-name {
      font-size: 13.5px;
      font-weight: 500;
      color: var(--text-primary);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    /* Files Grid View */
    .files-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(225px, 1fr));
      gap: 14px;
    }

    .file-card {
      background: var(--bg-shell);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      overflow: hidden;
      cursor: pointer;
      display: flex;
      flex-direction: column;
      transition: transform 0.12s, box-shadow 0.15s, background 0.15s;
    }

    .file-card:hover {
      background: #edf2fc;
      box-shadow: var(--shadow-card);
    }

    .file-card.selected {
      background: var(--bg-selected-soft);
      border-color: var(--brand-blue);
      box-shadow: 0 0 0 1px var(--brand-blue);
    }

    .file-card-top {
      padding: 10px 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }

    .file-card-title {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
      font-size: 13.5px;
      font-weight: 500;
      color: var(--text-primary);
    }

    .file-card-title span {
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .file-card-preview {
      height: 112px;
      margin: 0 10px;
      background: #ffffff;
      border-radius: 8px;
      border: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 7px;
      position: relative;
    }

    .file-card-footer {
      padding: 8px 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 12px;
      color: var(--text-muted);
    }

    /* Files List View (Google Drive Table) */
    .table-responsive {
      width: 100%;
      overflow-x: auto;
    }

    .drive-table {
      width: 100%;
      border-collapse: collapse;
    }

    .drive-table th {
      text-align: left;
      font-size: 12px;
      font-weight: 600;
      color: var(--text-secondary);
      padding: 9px 10px;
      border-bottom: 1px solid var(--border-color);
      white-space: nowrap;
    }

    .drive-table td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--border-subtle);
      font-size: 13px;
      color: var(--text-secondary);
      vertical-align: middle;
      white-space: nowrap;
    }

    .drive-table tr.item-row {
      cursor: pointer;
      transition: background 0.1s;
    }

    .drive-table tr.item-row:hover {
      background: var(--bg-hover);
    }

    .drive-table tr.item-row.selected {
      background: var(--bg-selected-soft);
    }

    .row-name-cell {
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--text-primary);
      font-weight: 500;
      max-width: 280px;
    }

    .row-name-cell span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .row-actions {
      display: inline-flex;
      align-items: center;
      justify-content: flex-end;
      gap: 2px;
      white-space: nowrap;
    }

    /* Status Badges */
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 2px 8px;
      border-radius: var(--radius-pill);
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.2px;
      white-space: nowrap;
    }

    .status-HEALTHY, .status-ALIVE {
      background: var(--status-healthy-bg);
      color: var(--status-healthy-fg);
    }

    .status-SUB_REPLICATED, .status-SUSPECT, .status-PENDING {
      background: var(--status-warn-bg);
      color: var(--status-warn-fg);
    }

    .status-RECOVERING, .status-WRITING, .status-COPYING {
      background: var(--status-recover-bg);
      color: var(--status-recover-fg);
    }

    .status-CORRUPTED, .status-DEAD {
      background: var(--status-danger-bg);
      color: var(--status-danger-fg);
    }

    .status-DRAINING {
      background: var(--status-drain-bg);
      color: var(--status-drain-fg);
    }

    .status-DIRECTORY {
      background: #f1f3f4;
      color: var(--text-secondary);
    }

    /* Right Inspector Drawer (Google Drive Details Pane) */
    .details-drawer {
      width: 332px;
      border-left: 1px solid var(--border-color);
      background: #ffffff;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      flex-shrink: 0;
      z-index: 20;
    }

    .details-drawer.hidden {
      display: none !important;
    }

    @media (max-width: 1150px) {
      .details-drawer {
        position: absolute;
        top: 0;
        right: 0;
        bottom: 0;
        box-shadow: -4px 0 18px rgba(0, 0, 0, 0.12);
      }
    }

    @media (max-width: 820px) {
      .sidebar {
        width: 200px;
      }
      .brand {
        min-width: auto;
      }
    }

    .drawer-header {
      padding: 14px 16px;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .drawer-title {
      font-size: 14.5px;
      font-weight: 600;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .drawer-body {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }

    .kv-list {
      display: flex;
      flex-direction: column;
      gap: 9px;
      background: var(--bg-shell);
      padding: 12px 14px;
      border-radius: var(--radius-md);
      border: 1px solid var(--border-color);
    }

    .kv-row {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .kv-label {
      font-size: 11px;
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
    }

    .kv-value {
      font-size: 12.5px;
      color: var(--text-primary);
      word-break: break-all;
    }

    .kv-value.mono {
      font-family: "Consolas", "SFMono-Regular", monospace;
      font-size: 11px;
      background: #ffffff;
      padding: 4px 6px;
      border-radius: 6px;
      border: 1px solid var(--border-color);
    }

    .block-card {
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 11px;
      background: #ffffff;
      display: flex;
      flex-direction: column;
      gap: 7px;
    }

    .block-card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 12.5px;
      font-weight: 600;
    }

    .replica-pill {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 5px 8px;
      border-radius: 6px;
      background: var(--bg-shell);
      font-size: 11.5px;
      border: 1px solid var(--border-subtle);
      gap: 6px;
    }

    /* Cluster Control Plane View */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }

    .kpi-card {
      background: var(--bg-shell);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 15px;
      display: flex;
      flex-direction: column;
      gap: 5px;
    }

    .kpi-label {
      font-size: 12px;
      color: var(--text-muted);
      font-weight: 600;
    }

    .kpi-value {
      font-size: 25px;
      font-weight: 600;
      color: var(--text-primary);
    }

    .nodes-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 14px;
    }

    .node-card {
      background: #ffffff;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 15px;
      display: flex;
      flex-direction: column;
      gap: 11px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }

    .node-card-header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
    }

    .disk-box {
      background: var(--bg-shell);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 9px 11px;
      display: flex;
      flex-direction: column;
      gap: 5px;
    }

    /* Empty state */
    .empty-state {
      text-align: center;
      padding: 52px 20px;
      color: var(--text-muted);
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
    }

    /* Modals */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.45);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 100;
    }

    .modal-backdrop.open {
      display: flex;
    }

    .modal {
      background: #ffffff;
      border-radius: 16px;
      width: 100%;
      max-width: 480px;
      padding: 24px;
      box-shadow: var(--shadow-modal);
      display: flex;
      flex-direction: column;
      gap: 15px;
    }

    .modal h3 {
      font-size: 18px;
      font-weight: 600;
      color: var(--text-primary);
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .form-group label {
      font-size: 13px;
      font-weight: 500;
      color: var(--text-secondary);
    }

    .form-input, .form-select, .form-textarea {
      width: 100%;
      padding: 9px 12px;
      border: 1px solid var(--border-color);
      border-radius: 8px;
      font-size: 14px;
      outline: none;
      font-family: inherit;
    }

    .form-input:focus, .form-select:focus, .form-textarea:focus {
      border-color: var(--brand-blue);
      box-shadow: 0 0 0 2px rgba(11, 87, 208, 0.15);
    }

    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      margin-top: 4px;
    }

    /* Toast Notification (Centered bottom so it never covers sidebar Armazenamento) */
    .toast-container {
      position: fixed;
      bottom: 20px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 200;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 8px;
      pointer-events: none;
    }

    .toast {
      background: #323232;
      color: #ffffff;
      padding: 10px 18px;
      border-radius: 8px;
      font-size: 13px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.25);
      display: flex;
      align-items: center;
      gap: 10px;
      max-width: 480px;
    }

    .drop-overlay {
      position: absolute;
      inset: 12px;
      background: rgba(232, 240, 254, 0.92);
      border: 2px dashed var(--brand-blue);
      border-radius: var(--radius-lg);
      display: none;
      align-items: center;
      justify-content: center;
      flex-direction: column;
      gap: 10px;
      z-index: 50;
      color: var(--brand-blue);
      font-weight: 600;
      font-size: 18px;
    }

    .drop-overlay.active {
      display: flex;
    }
  </style>
</head>
<body>
  <!-- Top Workspace Header -->
  <header class="topbar">
    <div class="brand" onclick="openMyDriveRoot()">
      <div class="brand-logo" aria-hidden="true">
        <svg width="34" height="34" viewBox="0 0 87.3 78" xmlns="http://www.w3.org/2000/svg">
          <path d="m6.6 66.85 3.85 6.65c.8 1.4 1.95 2.5 3.3 3.3l13.75-23.8h-27.5c0 1.55.4 3.1 1.2 4.5z" fill="#0066da"/>
          <path d="m43.65 25-13.75-23.8c-1.35.8-2.5 1.9-3.3 3.3l-25.4 44a9.06 9.06 0 0 0 -1.2 4.5h27.5z" fill="#00ac47"/>
          <path d="m73.55 76.8c1.35-.8 2.5-1.9 3.3-3.3l1.6-2.75 7.65-13.25c.8-1.4 1.2-2.95 1.2-4.5h-27.502l5.852 11.5z" fill="#ea4335"/>
          <path d="m43.65 25 13.75-23.8c-1.35-.8-2.9-1.2-4.5-1.2h-18.5c-1.6 0-3.15.45-4.5 1.2z" fill="#00832d"/>
          <path d="m59.8 53h-32.3l-13.75 23.8c1.35.8 2.9 1.2 4.5 1.2h50.8c1.6 0 3.15-.45 4.5-1.2z" fill="#2684fc"/>
          <path d="m73.4 26.5-12.7-22c-.8-1.4-1.95-2.5-3.3-3.3l-13.75 23.8 16.15 28h27.45c0-1.55-.4-3.1-1.2-4.5z" fill="#ffba00"/>
        </svg>
      </div>
      <div>
        <span class="brand-title">DComp Drive</span>
        <span class="brand-subtitle">UFS • SD</span>
      </div>
    </div>

    <div class="search-wrapper">
      <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="11" cy="11" r="8"></circle>
        <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
      </svg>
      <input
        id="searchInput"
        type="text"
        class="search-input"
        placeholder="Pesquisar em todo o DComp Drive (nome, caminho, SHA-256 ou status)..."
        oninput="renderDriveContent()"
      />
    </div>

    <div class="topbar-right">
      <div class="cluster-pill" onclick="switchTab('cluster')" title="Ver nós de armazenamento e discos">
        <span id="clusterDot" class="dot"></span>
        <span id="clusterNodesSummary">Nós: carregando...</span>
      </div>
      <div class="cluster-pill" onclick="triggerMonitorCycle()" title="Executar detecção de falhas e fila de re-replicação agora">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="23 4 23 10 17 10"></polyline>
          <polyline points="1 20 1 14 7 14"></polyline>
          <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
        </svg>
        <span id="replicationBadge">Re-replicações: 0</span>
      </div>
      <button class="icon-btn active" id="btnToggleDetails" onclick="toggleDetailsDrawer()" title="Mostrar/ocultar painel de detalhes">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <line x1="12" y1="16" x2="12" y2="12"></line>
          <line x1="12" y1="8" x2="12.01" y2="8"></line>
        </svg>
      </button>
    </div>
  </header>

  <!-- Main Workspace -->
  <div class="workspace">
    <!-- Left Google Drive Sidebar -->
    <aside class="sidebar">
      <button class="btn-new" onclick="openUploadModal()">
        <svg width="24" height="24" viewBox="0 0 24 24">
          <path fill="#34A853" d="M11 11V5h2v6h6v2h-6v6h-2v-6H5v-2z"/>
        </svg>
        <span>Novo</span>
      </button>

      <nav class="nav-list">
        <button class="nav-item active" id="nav-drive" onclick="openMyDriveRoot()">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path>
          </svg>
          <span>Meu Drive</span>
        </button>

        <button class="nav-item" id="nav-cluster" onclick="switchTab('cluster')">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
            <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
            <line x1="6" y1="6" x2="6.01" y2="6"></line>
            <line x1="6" y1="18" x2="6.01" y2="18"></line>
          </svg>
          <span>Cluster & Nós</span>
          <span class="nav-badge">RF5–7</span>
        </button>

        <button class="nav-item" id="nav-encrypted" onclick="filterEncryptedFromNav()">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect>
            <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
          </svg>
          <span>Cifrados (AES-GCM)</span>
        </button>

        <button class="nav-item" id="nav-subreplicated" onclick="filterSubReplicatedFromNav()">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
            <line x1="12" y1="9" x2="12" y2="13"></line>
            <line x1="12" y1="17" x2="12.01" y2="17"></line>
          </svg>
          <span>Saúde & Re-replicação</span>
        </button>
      </nav>

      <div class="storage-box">
        <div class="storage-header">
          <span>Armazenamento</span>
          <span id="driveLogicalBytesLabel" style="color: var(--brand-blue);">0 B</span>
        </div>
        <div class="progress-track">
          <div id="storageProgressBar" class="progress-fill" style="width: 0%"></div>
        </div>
        <div id="storageCaption" class="storage-caption">
          Calculando capacidade dos volumes...
        </div>
        <div class="storage-meta-badges">
          <span class="mini-tag">N = 3 réplicas</span>
          <span class="mini-tag">Quórum W = 2</span>
          <span class="mini-tag">Limite 90%</span>
        </div>
      </div>
    </aside>

    <!-- Single Semantic Main Landmark -->
    <main class="canvas-container" id="dropTarget">
      <div class="drop-overlay" id="dropOverlay">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
          <polyline points="17 8 12 3 7 8"></polyline>
          <line x1="12" y1="3" x2="12" y2="15"></line>
        </svg>
        <span>Solte o arquivo aqui para enviar ao DComp Drive</span>
      </div>

      <!-- TAB 1: MEU DRIVE -->
      <section class="main-view" id="view-drive">
        <div class="drive-header">
          <div class="breadcrumbs" id="breadcrumbsBar"></div>

          <div class="header-actions">
            <button class="btn btn-tonal" onclick="openMkdirModal()">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>
                <line x1="12" y1="11" x2="12" y2="17"></line>
                <line x1="9" y1="14" x2="15" y2="14"></line>
              </svg>
              <span>Nova pasta</span>
            </button>

            <button class="btn btn-primary" onclick="openUploadModal()">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                <polyline points="17 8 12 3 7 8"></polyline>
                <line x1="12" y1="3" x2="12" y2="15"></line>
              </svg>
              <span>Upload de arquivo</span>
            </button>

            <button class="btn" onclick="refreshAll()" title="Sincronizar com o Controlador">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="23 4 23 10 17 10"></polyline>
                <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path>
              </svg>
            </button>

            <div class="view-toggle">
              <button id="btnViewList" class="active" onclick="setViewMode('list')" title="Visualização em lista">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="8" y1="6" x2="21" y2="6"></line>
                  <line x1="8" y1="12" x2="21" y2="12"></line>
                  <line x1="8" y1="18" x2="21" y2="18"></line>
                  <line x1="3" y1="6" x2="3.01" y2="6"></line>
                  <line x1="3" y1="12" x2="3.01" y2="12"></line>
                  <line x1="3" y1="18" x2="3.01" y2="18"></line>
                </svg>
              </button>
              <button id="btnViewGrid" onclick="setViewMode('grid')" title="Visualização em grade">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="3" width="7" height="7"></rect>
                  <rect x="14" y="3" width="7" height="7"></rect>
                  <rect x="14" y="14" width="7" height="7"></rect>
                  <rect x="3" y="14" width="7" height="7"></rect>
                </svg>
              </button>
            </div>
          </div>
        </div>

        <div class="chips-bar">
          <button class="chip active" data-filter="ALL" onclick="setFilter('ALL')">Todos os itens</button>
          <button class="chip" data-filter="HEALTHY" onclick="setFilter('HEALTHY')">Íntegros (HEALTHY)</button>
          <button class="chip" data-filter="SUB_REPLICATED" onclick="setFilter('SUB_REPLICATED')">Sub-replicados</button>
          <button class="chip" data-filter="RECOVERING" onclick="setFilter('RECOVERING')">Em Recuperação</button>
          <button class="chip" data-filter="CORRUPTED" onclick="setFilter('CORRUPTED')">Corrompidos</button>
          <button class="chip" data-filter="ENCRYPTED" onclick="setFilter('ENCRYPTED')">🔒 Cifrados AES-256-GCM</button>
        </div>

        <div class="content-scroll" id="driveContentArea"></div>
      </section>

      <!-- TAB 2: CLUSTER & TOLERÂNCIA A FALHAS -->
      <section class="main-view" id="view-cluster" style="display: none;">
        <div class="drive-header">
          <div>
            <h2 style="font-size: 18px; font-weight: 600;">Plano de Controle — Cluster, Alocação e Tolerância a Falhas</h2>
            <p style="font-size: 12.5px; color: var(--text-muted); margin-top: 2px;">
              Monitoramento de heartbeats (ALIVE / SUSPECT / DEAD / DRAINING), ocupação por disco (limite 90%) e fila priorizada de re-replicação.
            </p>
          </div>
          <div class="header-actions">
            <button class="btn btn-tonal" onclick="triggerMonitorCycle()">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5 3 19 12 5 21 5 3"></polygon>
              </svg>
              <span>Executar Ciclo do Monitor</span>
            </button>
            <button class="btn" onclick="refreshAll()">Atualizar Estado</button>
          </div>
        </div>

        <div class="content-scroll">
          <div class="kpi-grid" id="clusterKpiGrid"></div>
          <div class="section-label">
            <span>Nós de Armazenamento e Discos Físicos</span>
            <span id="clusterLastUpdated" style="font-weight: 400; font-size: 12px; color: var(--text-muted);"></span>
          </div>
          <div class="nodes-grid" id="clusterNodesGrid"></div>
        </div>
      </section>

      <!-- Right Details Drawer -->
      <aside class="details-drawer" id="detailsDrawer">
        <div class="drawer-header">
          <div class="drawer-title">
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"></circle>
              <line x1="12" y1="16" x2="12" y2="12"></line>
              <line x1="12" y1="8" x2="12.01" y2="8"></line>
            </svg>
            <span id="drawerTitleText">Detalhes do Drive</span>
          </div>
          <button class="icon-btn" style="width:30px; height:30px;" onclick="toggleDetailsDrawer()" title="Fechar painel">
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>
        <div class="drawer-body" id="drawerBody"></div>
      </aside>
    </main>
  </div>

  <!-- Modal: Nova Pasta -->
  <div class="modal-backdrop" id="mkdirModal">
    <div class="modal">
      <h3>Nova pasta</h3>
      <div class="form-group">
        <label for="mkdirNameInput">Nome da pasta</label>
        <input id="mkdirNameInput" type="text" class="form-input" placeholder="Ex: relatorios-2026" />
      </div>
      <div class="modal-actions">
        <button class="btn" onclick="closeModal('mkdirModal')">Cancelar</button>
        <button class="btn btn-primary" onclick="submitMkdir()">Criar pasta</button>
      </div>
    </div>
  </div>

  <!-- Modal: Mover / Renomear -->
  <div class="modal-backdrop" id="moveModal">
    <div class="modal">
      <h3>Renomear ou mover item</h3>
      <input id="moveSourceInput" type="hidden" />
      <div class="form-group">
        <label for="moveDestInput">Novo caminho absoluto</label>
        <input id="moveDestInput" type="text" class="form-input" />
      </div>
      <div class="modal-actions">
        <button class="btn" onclick="closeModal('moveModal')">Cancelar</button>
        <button class="btn btn-primary" onclick="submitMove()">Salvar</button>
      </div>
    </div>
  </div>

  <!-- Modal: Upload de Arquivo -->
  <div class="modal-backdrop" id="uploadModal">
    <div class="modal">
      <h3>Upload para o DComp Drive</h3>
      <div class="form-group">
        <label for="uploadFileInput">Selecionar arquivo do computador</label>
        <input id="uploadFileInput" type="file" class="form-input" onchange="onUploadFileSelected()" />
      </div>
      <div class="form-group">
        <label for="uploadTextContent">Ou digite conteúdo rápido para criar um arquivo de teste</label>
        <textarea id="uploadTextContent" rows="3" class="form-textarea" placeholder="Opcional: se nenhum arquivo for escolhido acima, este texto será gravado como arquivo distribuído."></textarea>
      </div>
      <div class="form-group">
        <label for="uploadRemotePath">Caminho de destino no DComp Drive</label>
        <input id="uploadRemotePath" type="text" class="form-input" placeholder="/documento.txt" />
      </div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
        <div class="form-group">
          <label for="uploadReplicationTarget">Fator de Replicação (N)</label>
          <select id="uploadReplicationTarget" class="form-select">
            <option value="3" selected>N = 3 (Padrão)</option>
            <option value="2">N = 2</option>
            <option value="1">N = 1</option>
            <option value="4">N = 4</option>
            <option value="5">N = 5</option>
          </select>
        </div>
        <div class="form-group">
          <label for="uploadEncryptToggle">Criptografia no Cliente</label>
          <select id="uploadEncryptToggle" class="form-select">
            <option value="false" selected>Desativada (Texto claro)</option>
            <option value="true">AES-256-GCM (Ativa)</option>
          </select>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn" onclick="closeModal('uploadModal')">Cancelar</button>
        <button class="btn btn-primary" id="btnSubmitUpload" onclick="submitUpload()">Enviar para o Cluster</button>
      </div>
    </div>
  </div>

  <div class="toast-container" id="toastContainer"></div>

  <script>
    let currentPath = "/";
    let currentEntries = [];
    let allDriveEntries = [];
    let totalLogicalBytes = 0;
    let selectedEntryPath = null;
    let selectedMetadata = null;
    let clusterState = null;
    let viewMode = "list";
    let statusFilter = "ALL";
    let activeTab = "drive";
    let drawerUserOpen = window.innerWidth > 1150;

    function formatBytes(bytes) {
      if (bytes === 0 || bytes === null || bytes === undefined) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      const idx = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
      const val = bytes / Math.pow(1024, idx);
      return `${val.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
    }

    function formatDate(iso) {
      if (!iso) return "-";
      try {
        const d = new Date(iso);
        return d.toLocaleString("pt-BR", {
          day: "2-digit",
          month: "short",
          hour: "2-digit",
          minute: "2-digit",
        });
      } catch {
        return iso;
      }
    }

    function showToast(msg) {
      const container = document.getElementById("toastContainer");
      const el = document.createElement("div");
      el.className = "toast";
      el.textContent = msg;
      container.appendChild(el);
      setTimeout(() => {
        el.remove();
      }, 3800);
    }

    function syncDrawerVisibility() {
      const drawer = document.getElementById("detailsDrawer");
      const btn = document.getElementById("btnToggleDetails");
      const shouldShow = activeTab === "drive" && drawerUserOpen;
      drawer.classList.toggle("hidden", !shouldShow);
      btn.classList.toggle("active", shouldShow);
    }

    function openMyDriveRoot() {
      switchTab("drive");
      setFilter("ALL", true);
    }

    function switchTab(tab) {
      activeTab = tab;
      document.getElementById("view-drive").style.display = tab === "drive" ? "flex" : "none";
      document.getElementById("view-cluster").style.display = tab === "cluster" ? "flex" : "none";
      document.querySelectorAll(".nav-item").forEach(btn => btn.classList.remove("active"));
      if (tab === "drive") {
        document.getElementById("nav-drive").classList.add("active");
      } else {
        document.getElementById("nav-cluster").classList.add("active");
      }
      syncDrawerVisibility();
    }

    function filterEncryptedFromNav() {
      switchTab("drive");
      setFilter("ENCRYPTED", false);
      document.querySelectorAll(".nav-item").forEach(btn => btn.classList.remove("active"));
      document.getElementById("nav-encrypted").classList.add("active");
    }

    function filterSubReplicatedFromNav() {
      switchTab("drive");
      setFilter("SUB_REPLICATED", false);
      document.querySelectorAll(".nav-item").forEach(btn => btn.classList.remove("active"));
      document.getElementById("nav-subreplicated").classList.add("active");
    }

    function setViewMode(mode) {
      viewMode = mode;
      document.getElementById("btnViewList").classList.toggle("active", mode === "list");
      document.getElementById("btnViewGrid").classList.toggle("active", mode === "grid");
      renderDriveContent();
    }

    function setFilter(filter, syncNav = true) {
      statusFilter = filter;
      document.querySelectorAll(".chip").forEach(c => {
        c.classList.toggle("active", c.getAttribute("data-filter") === filter);
      });
      if (syncNav && activeTab === "drive") {
        document.querySelectorAll(".nav-item").forEach(btn => btn.classList.remove("active"));
        if (filter === "ENCRYPTED") {
          document.getElementById("nav-encrypted").classList.add("active");
        } else if (filter === "SUB_REPLICATED" || filter === "RECOVERING" || filter === "CORRUPTED") {
          document.getElementById("nav-subreplicated").classList.add("active");
        } else {
          document.getElementById("nav-drive").classList.add("active");
        }
      }
      renderBreadcrumbs();
      renderDriveContent();
    }

    function toggleDetailsDrawer() {
      if (activeTab !== "drive") {
        switchTab("drive");
        drawerUserOpen = true;
      } else {
        drawerUserOpen = !drawerUserOpen;
      }
      syncDrawerVisibility();
    }

    function renderBreadcrumbs() {
      const bar = document.getElementById("breadcrumbsBar");
      bar.innerHTML = "";

      const rootBtn = document.createElement("button");
      rootBtn.className = "crumb" + (currentPath === "/" && statusFilter === "ALL" ? " current" : "");
      rootBtn.textContent = "Meu Drive";
      rootBtn.onclick = () => {
        setFilter("ALL", true);
        navigateTo("/");
      };
      bar.appendChild(rootBtn);

      if (statusFilter !== "ALL") {
        const sep = document.createElement("span");
        sep.className = "crumb-sep";
        sep.textContent = "›";
        bar.appendChild(sep);

        const filterBtn = document.createElement("button");
        filterBtn.className = "crumb current";
        const labels = {
          HEALTHY: "Arquivos Íntegros (Global)",
          SUB_REPLICATED: "Arquivos Sub-replicados (Global)",
          RECOVERING: "Em Recuperação (Global)",
          CORRUPTED: "Arquivos Corrompidos (Global)",
          ENCRYPTED: "Cifrados AES-256-GCM (Global)",
        };
        filterBtn.textContent = labels[statusFilter] || statusFilter;
        bar.appendChild(filterBtn);
        return;
      }

      const parts = currentPath.split("/").filter(Boolean);
      let acc = "";
      parts.forEach((part, idx) => {
        acc += "/" + part;
        const target = acc;
        const sep = document.createElement("span");
        sep.className = "crumb-sep";
        sep.textContent = "›";
        bar.appendChild(sep);

        const btn = document.createElement("button");
        btn.className = "crumb" + (idx === parts.length - 1 ? " current" : "");
        btn.textContent = part;
        btn.onclick = () => navigateTo(target);
        bar.appendChild(btn);
      });
    }

    async function navigateTo(path) {
      currentPath = path || "/";
      if (statusFilter !== "ALL") {
        statusFilter = "ALL";
        document.querySelectorAll(".chip").forEach(c => {
          c.classList.toggle("active", c.getAttribute("data-filter") === "ALL");
        });
      }
      selectedEntryPath = null;
      selectedMetadata = null;
      renderBreadcrumbs();
      await loadDirectory(currentPath);
      renderDetailsDrawer();
    }

    async function loadDirectory(path) {
      try {
        const [dirRes, allRes] = await Promise.all([
          fetch(`/dirs?path=${encodeURIComponent(path)}`),
          fetch("/ui/api/entries"),
        ]);
        if (dirRes.ok) {
          const data = await dirRes.json();
          currentEntries = data.entries || [];
        }
        if (allRes.ok) {
          const allData = await allRes.json();
          allDriveEntries = allData.entries || [];
          totalLogicalBytes = allData.total_logical_bytes || 0;
        }
        renderDriveContent();
        renderClusterSummary();
        if (!selectedMetadata) {
          renderDetailsDrawer();
        }
      } catch (e) {
        showToast(`Erro ao carregar diretório: ${e.message}`);
      }
    }

    async function loadClusterStatus() {
      try {
        const res = await fetch("/cluster/status");
        if (!res.ok) return;
        clusterState = await res.json();
        renderClusterSummary();
        renderClusterView();
        if (!selectedMetadata) {
          renderDetailsDrawer();
        }
      } catch (e) {
        console.error(e);
      }
    }

    function renderClusterSummary() {
      document.getElementById("driveLogicalBytesLabel").textContent = formatBytes(totalLogicalBytes);
      if (!clusterState) return;
      const nodes = clusterState.nodes || [];
      const aliveCount = nodes.filter(n => n.status === "ALIVE").length;
      const suspectCount = nodes.filter(n => n.status === "SUSPECT").length;
      const deadCount = nodes.filter(n => n.status === "DEAD").length;

      const dot = document.getElementById("clusterDot");
      dot.className = "dot" + (deadCount > 0 ? " danger" : suspectCount > 0 ? " warn" : "");
      document.getElementById("clusterNodesSummary").textContent =
        `Nós Vivos: ${aliveCount}/${nodes.length}`;
      document.getElementById("replicationBadge").textContent =
        `Re-replicações: ${clusterState.active_re_replications || 0}`;

      let totalBytes = 0;
      let usedBytes = 0;
      nodes.forEach(n => {
        (n.disks || []).forEach(d => {
          if (d.is_active) {
            totalBytes += d.total_bytes || 0;
            usedBytes += d.used_bytes || 0;
          }
        });
      });

      const fileCount = allDriveEntries.filter(e => !e.is_dir).length;
      const bar = document.getElementById("storageProgressBar");
      const visualPct = Math.min(100, Math.max(fileCount > 0 ? 6 : 0, (totalLogicalBytes / (64 * 1024 * 1024)) * 100));
      bar.style.width = `${visualPct}%`;
      bar.className = "progress-fill";
      document.getElementById("storageCaption").textContent =
        `${fileCount} arquivo(s) no Drive • Capacidade bruta dos ${nodes.length} nós: ${formatBytes(totalBytes - usedBytes)} livres`;
    }

    function renderClusterView() {
      if (!clusterState) return;
      const nodes = clusterState.nodes || [];
      const filesByStatus = clusterState.files_by_status || {};
      const kpiGrid = document.getElementById("clusterKpiGrid");

      const alive = nodes.filter(n => n.status === "ALIVE").length;
      const suspect = nodes.filter(n => n.status === "SUSPECT").length;
      const dead = nodes.filter(n => n.status === "DEAD").length;
      const draining = nodes.filter(n => n.status === "DRAINING").length;

      kpiGrid.innerHTML = `
        <div class="kpi-card">
          <span class="kpi-label">NÓS ALIVE / TOTAL</span>
          <span class="kpi-value" style="color: var(--status-healthy-fg);">${alive} / ${nodes.length}</span>
          <span style="font-size: 12px; color: var(--text-muted);">Suspeitos: ${suspect} • Mortos: ${dead} • Draining: ${draining}</span>
        </div>
        <div class="kpi-card">
          <span class="kpi-label">ARQUIVOS ÍNTEGROS (HEALTHY)</span>
          <span class="kpi-value">${filesByStatus.HEALTHY || 0}</span>
          <span style="font-size: 12px; color: var(--text-muted);">Com todas as N réplicas verificadas</span>
        </div>
        <div class="kpi-card">
          <span class="kpi-label">SUB-REPLICADOS / RECUPERANDO</span>
          <span class="kpi-value" style="color: var(--status-warn-fg);">${(filesByStatus.SUB_REPLICATED || 0) + (filesByStatus.RECOVERING || 0)}</span>
          <span style="font-size: 12px; color: var(--text-muted);">Cópias ativas: ${clusterState.active_re_replications || 0}</span>
        </div>
        <div class="kpi-card">
          <span class="kpi-label">CORROMPIDOS (CORRUPTED)</span>
          <span class="kpi-value" style="color: var(--status-danger-fg);">${filesByStatus.CORRUPTED || 0}</span>
          <span style="font-size: 12px; color: var(--text-muted);">Scrubbing & verificação SHA-256</span>
        </div>
      `;

      document.getElementById("clusterLastUpdated").textContent =
        `Atualizado às ${new Date().toLocaleTimeString("pt-BR")}`;

      const nodesGrid = document.getElementById("clusterNodesGrid");
      if (nodes.length === 0) {
        nodesGrid.innerHTML = `
          <div class="empty-state" style="grid-column: 1 / -1;">
            <p>Nenhum Nó de Armazenamento registrado no momento.</p>
          </div>
        `;
        return;
      }

      nodesGrid.innerHTML = nodes.map(node => {
        const disksHtml = (node.disks || []).map(d => {
          const pct = d.usage_pct || 0;
          const barClass = pct >= 90 ? "danger" : pct >= 75 ? "warn" : "";
          return `
            <div class="disk-box">
              <div style="display:flex; justify-content:space-between; font-size:12px; font-weight:600;">
                <span>💽 ${d.disk_id}</span>
                <span>${pct.toFixed(1)}% (${d.is_active ? "Ativo" : "Inativo"})</span>
              </div>
              <div class="progress-track">
                <div class="progress-fill ${barClass}" style="width: ${Math.min(100, pct)}%"></div>
              </div>
              <div style="display:flex; justify-content:space-between; font-size:11px; color:var(--text-muted);">
                <span>Usado: ${formatBytes(d.used_bytes)}</span>
                <span>Livre: ${formatBytes(d.free_bytes)} / ${formatBytes(d.total_bytes)}</span>
              </div>
            </div>
          `;
        }).join("");

        return `
          <div class="node-card">
            <div class="node-card-header">
              <div>
                <div style="font-size: 15px; font-weight: 600; color: var(--text-primary);">${node.node_id}</div>
                <div style="font-size: 12px; color: var(--text-muted);">Máquina: <strong>${node.machine_id}</strong> • ${node.host_url}</div>
              </div>
              <span class="status-badge status-${node.status}">${node.status}</span>
            </div>
            <div style="font-size: 12px; color: var(--text-muted);">
              Último heartbeat: <strong>${formatDate(node.last_heartbeat)}</strong>
            </div>
            <div style="display:flex; flex-direction:column; gap:8px;">
              ${disksHtml}
            </div>
            <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:auto; padding-top:4px;">
              ${node.status !== "DRAINING" ? `
                <button class="btn" style="height:30px; font-size:12px;" onclick="drainNode('${node.node_id}')">
                  Esvaziar Nó (Drain)
                </button>
              ` : `
                <span style="font-size:12px; color:var(--status-drain-fg); font-weight:500;">Esvaziamento planejado ativo</span>
              `}
            </div>
          </div>
        `;
      }).join("");
    }

    function renderDriveContent() {
      const container = document.getElementById("driveContentArea");
      const query = (document.getElementById("searchInput").value || "").trim().toLowerCase();

      // Quando há busca ou filtro global de status/criptografia, pesquisa em toda a árvore do Drive
      const isGlobalMode = Boolean(query) || statusFilter !== "ALL";
      const sourceList = isGlobalMode ? allDriveEntries : currentEntries;

      const filtered = sourceList.filter(item => {
        if (query) {
          const matchName = item.name.toLowerCase().includes(query);
          const matchPath = item.path.toLowerCase().includes(query);
          const matchHash = (item.file_hash || "").toLowerCase().includes(query);
          const matchStatus = (item.status || "").toLowerCase().includes(query);
          if (!matchName && !matchPath && !matchHash && !matchStatus) return false;
        }
        if (statusFilter === "ALL") return true;
        if (statusFilter === "ENCRYPTED") return !item.is_dir && item.is_encrypted === true;
        return !item.is_dir && item.status === statusFilter;
      });

      const folders = filtered.filter(e => e.is_dir);
      const files = filtered.filter(e => !e.is_dir);

      if (folders.length === 0 && files.length === 0) {
        if (isGlobalMode) {
          container.innerHTML = `
            <div class="empty-state">
              <svg width="56" height="56" viewBox="0 0 24 24" fill="none" stroke="#c4c7c5" stroke-width="1.5">
                <circle cx="11" cy="11" r="8"></circle>
                <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
              </svg>
              <h3 style="font-size: 16px; color: var(--text-secondary); font-weight: 500;">
                Nenhum item encontrado para o filtro atual
              </h3>
              <p style="font-size: 13px; max-width: 400px;">
                Não há arquivos no cluster com o filtro <strong>${statusFilter}</strong>${query ? ` ou termo "${query}"` : ""}.
              </p>
              <div style="display:flex; gap:10px; margin-top:6px;">
                <button class="btn btn-tonal" onclick="clearSearchAndFilters()">Mostrar todos os itens</button>
              </div>
            </div>
          `;
        } else {
          container.innerHTML = `
            <div class="empty-state">
              <svg width="60" height="60" viewBox="0 0 24 24" fill="none" stroke="#c4c7c5" stroke-width="1.5">
                <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>
              </svg>
              <h3 style="font-size: 16.5px; color: var(--text-secondary); font-weight: 500;">Esta pasta está vazia</h3>
              <p style="font-size: 13px; max-width: 420px;">
                Arraste arquivos para cá ou clique em <strong>+ Novo</strong> para dividir arquivos em blocos distribuídos com verificação SHA-256 e replicação tolerante a falhas.
              </p>
              <div style="display:flex; gap:10px; margin-top:6px;">
                <button class="btn btn-primary" onclick="openUploadModal()">Fazer upload de arquivo</button>
                <button class="btn btn-tonal" onclick="createSampleDemoFile()">Gerar arquivo de demonstração</button>
              </div>
            </div>
          `;
        }
        return;
      }

      let html = "";

      if (folders.length > 0) {
        html += `<div class="section-label"><span>Pastas (${folders.length})</span></div>`;
        html += `<div class="folders-grid">`;
        folders.forEach(f => {
          const isSel = selectedEntryPath === f.path;
          const displayLabel = isGlobalMode ? f.path : f.name;
          html += `
            <div class="folder-card ${isSel ? "selected" : ""}"
                 onclick="selectItem('${f.path}')"
                 ondblclick="navigateTo('${f.path}')">
              <div class="folder-card-left">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="#5f6368">
                  <path d="M10 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/>
                </svg>
                <span class="folder-name" title="${f.path}">${displayLabel}</span>
              </div>
              <div class="row-actions" onclick="event.stopPropagation()">
                <button class="icon-btn" style="width:26px; height:26px;" onclick="navigateTo('${f.path}')" title="Abrir pasta">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="9 18 15 12 9 6"></polyline>
                  </svg>
                </button>
                <button class="icon-btn" style="width:26px; height:26px;" onclick="openMoveModal('${f.path}')" title="Renomear / Mover">
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 20h9"></path>
                    <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"></path>
                  </svg>
                </button>
                <button class="icon-btn" style="width:26px; height:26px;" onclick="deleteEntry('${f.path}', true)" title="Excluir pasta">
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="3 6 5 6 21 6"></polyline>
                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                  </svg>
                </button>
              </div>
            </div>
          `;
        });
        html += `</div>`;
      }

      if (files.length > 0) {
        html += `<div class="section-label"><span>Arquivos (${files.length})</span></div>`;
        if (viewMode === "grid") {
          html += `<div class="files-grid">`;
          files.forEach(file => {
            const isSel = selectedEntryPath === file.path;
            const displayLabel = isGlobalMode ? file.path : file.name;
            html += `
              <div class="file-card ${isSel ? "selected" : ""}" onclick="selectItem('${file.path}')">
                <div class="file-card-top">
                  <div class="file-card-title">
                    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#0b57d0" stroke-width="2">
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                      <polyline points="14 2 14 8 20 8"></polyline>
                    </svg>
                    <span title="${file.path}">${displayLabel}</span>
                  </div>
                  ${file.is_encrypted ? `<span title="Cifrado com AES-256-GCM">🔒</span>` : ""}
                </div>
                <div class="file-card-preview">
                  <span class="status-badge status-${file.status}">${file.status}</span>
                  <span style="font-size: 12px; color: var(--text-muted);">Replicação N=${file.replication_target}</span>
                  <span style="font-size: 11px; font-family: monospace; color: var(--text-muted);">
                    SHA-256: ${(file.file_hash || "").slice(0, 12)}...
                  </span>
                </div>
                <div class="file-card-footer">
                  <span>${formatBytes(file.size_bytes)}</span>
                  <div class="row-actions" onclick="event.stopPropagation()">
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="downloadFile('${file.path}')" title="Baixar arquivo">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                        <polyline points="7 10 12 15 17 10"></polyline>
                        <line x1="12" y1="15" x2="12" y2="3"></line>
                      </svg>
                    </button>
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="openMoveModal('${file.path}')" title="Renomear / Mover">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M12 20h9"></path>
                        <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"></path>
                      </svg>
                    </button>
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="deleteEntry('${file.path}', false)" title="Excluir">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="3 6 5 6 21 6"></polyline>
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"></path>
                      </svg>
                    </button>
                  </div>
                </div>
              </div>
            `;
          });
          html += `</div>`;
        } else {
          html += `
            <div class="table-responsive">
              <table class="drive-table">
                <thead>
                  <tr>
                    <th>Nome</th>
                    <th>Estado & Saúde</th>
                    <th>Criptografia</th>
                    <th>Fator N</th>
                    <th>Tamanho</th>
                    <th>Modificado</th>
                    <th style="text-align:right;">Ações</th>
                  </tr>
                </thead>
                <tbody>
          `;
          files.forEach(file => {
            const isSel = selectedEntryPath === file.path;
            const displayLabel = isGlobalMode ? file.path : file.name;
            html += `
              <tr class="item-row ${isSel ? "selected" : ""}" onclick="selectItem('${file.path}')">
                <td>
                  <div class="row-name-cell" title="${file.path}">
                    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#0b57d0" stroke-width="2">
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                      <polyline points="14 2 14 8 20 8"></polyline>
                    </svg>
                    <span>${displayLabel}</span>
                  </div>
                </td>
                <td><span class="status-badge status-${file.status}">${file.status}</span></td>
                <td>${file.is_encrypted ? "🔒 AES-256-GCM" : "Texto claro"}</td>
                <td>N = ${file.replication_target}</td>
                <td>${formatBytes(file.size_bytes)}</td>
                <td>${formatDate(file.updated_at)}</td>
                <td style="text-align:right;" onclick="event.stopPropagation()">
                  <div class="row-actions">
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="downloadFile('${file.path}')" title="Download verificado (SHA-256)">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                        <polyline points="7 10 12 15 17 10"></polyline>
                        <line x1="12" y1="15" x2="12" y2="3"></line>
                      </svg>
                    </button>
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="openMoveModal('${file.path}')" title="Mover ou renomear">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M12 20h9"></path>
                        <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"></path>
                      </svg>
                    </button>
                    <button class="icon-btn" style="width:28px; height:28px;" onclick="deleteEntry('${file.path}', false)" title="Remover arquivo">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="3 6 5 6 21 6"></polyline>
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"></path>
                      </svg>
                    </button>
                  </div>
                </td>
              </tr>
            `;
          });
          html += `</tbody></table></div>`;
        }
      }

      container.innerHTML = html;
    }

    function clearSearchAndFilters() {
      document.getElementById("searchInput").value = "";
      setFilter("ALL", true);
    }

    async function selectItem(path) {
      selectedEntryPath = path;
      renderDriveContent();
      try {
        const res = await fetch(`/files/metadata?path=${encodeURIComponent(path)}&include_all_replicas=true`);
        if (!res.ok) return;
        selectedMetadata = await res.json();
        renderDetailsDrawer();
      } catch (e) {
        console.error(e);
      }
    }

    function renderDetailsDrawer() {
      const body = document.getElementById("drawerBody");
      const title = document.getElementById("drawerTitleText");

      if (!selectedMetadata) {
        title.textContent = "Visão Geral do Drive";
        const filesByStatus = (clusterState && clusterState.files_by_status) || {};
        body.innerHTML = `
          <div style="text-align:center; padding: 14px 8px;">
            <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="#0b57d0" stroke-width="1.5">
              <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>
            </svg>
            <div style="font-size:14.5px; font-weight:600; margin-top:6px;">${currentPath === "/" ? "Meu Drive (Raiz)" : currentPath}</div>
            <div style="font-size:12px; color:var(--text-muted); margin-top:4px;">
              Clique em um arquivo ou pasta para inspecionar seus metadados, hashes SHA-256, blocos e distribuição das réplicas nos nós.
            </div>
          </div>
          <div class="kv-list">
            <div class="kv-row">
              <span class="kv-label">Itens nesta pasta</span>
              <span class="kv-value">${currentEntries.length} item(ns)</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">Total armazenado no Drive</span>
              <span class="kv-value">${formatBytes(totalLogicalBytes)}</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">Arquivos HEALTHY no Cluster</span>
              <span class="kv-value">${filesByStatus.HEALTHY || 0}</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">Arquivos SUB_REPLICATED / RECOVERING</span>
              <span class="kv-value">${(filesByStatus.SUB_REPLICATED || 0) + (filesByStatus.RECOVERING || 0)}</span>
            </div>
          </div>
        `;
        return;
      }

      const m = selectedMetadata;
      title.textContent = m.is_dir ? "Detalhes da Pasta" : "Metadados e Réplicas";

      let blocksHtml = "";
      if (!m.is_dir) {
        const blocks = m.blocks || [];
        blocksHtml = `
          <div style="font-size:12.5px; font-weight:600; color:var(--text-secondary); margin-top:2px;">
            Mapa de Blocos e Réplicas (${blocks.length})
          </div>
        `;
        blocks.forEach(b => {
          const reps = (b.replicas || []).map(r => `
            <div class="replica-pill">
              <div style="min-width:0; overflow:hidden; text-overflow:ellipsis;">
                <strong>${r.node_id}</strong>
                <span style="color:var(--text-muted);">(${r.machine_id} • ${r.disk_id})</span>
              </div>
              <div style="display:flex; gap:4px; flex-shrink:0;">
                <span class="status-badge status-${r.status}">${r.status}</span>
                <span class="status-badge status-${r.node_status}">${r.node_status}</span>
              </div>
            </div>
          `).join("");

          blocksHtml += `
            <div class="block-card">
              <div class="block-card-header">
                <span>Bloco #${b.block_index}</span>
                <span>${formatBytes(b.size_bytes)}</span>
              </div>
              <div style="font-size:11px; color:var(--text-muted); font-family:monospace; word-break:break-all;">
                ID: ${b.block_id}<br/>
                SHA-256: ${b.block_hash}
                ${b.encryption_nonce ? `<br/>Nonce GCM: ${b.encryption_nonce}` : ""}
              </div>
              <div style="display:flex; flex-direction:column; gap:5px; margin-top:3px;">
                ${reps || `<span style="font-size:12px; color:var(--status-danger-fg);">Sem réplicas registradas</span>`}
              </div>
            </div>
          `;
        });
      }

      body.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:space-between; gap:8px;">
          <span class="status-badge status-${m.status}">${m.status}</span>
          ${!m.is_dir ? `
            <button class="btn btn-primary" style="height:30px; font-size:12px;" onclick="downloadFile('${m.path}')">
              Baixar Arquivo
            </button>
          ` : ""}
        </div>

        <div class="kv-list">
          <div class="kv-row">
            <span class="kv-label">Caminho Absoluto</span>
            <span class="kv-value">${m.path}</span>
          </div>
          <div class="kv-row">
            <span class="kv-label">Entry UUID</span>
            <span class="kv-value mono">${m.entry_id}</span>
          </div>
          ${!m.is_dir ? `
            <div class="kv-row">
              <span class="kv-label">Tamanho Original</span>
              <span class="kv-value">${formatBytes(m.size_bytes)} (${m.size_bytes} bytes)</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">Criptografia (RF9)</span>
              <span class="kv-value">${m.is_encrypted ? "🔒 AES-256-GCM (Chave no Cliente)" : "Texto Claro"}</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">Fator de Replicação Alvo (N)</span>
              <span class="kv-value">N = ${m.replication_target} réplicas distintas</span>
            </div>
            <div class="kv-row">
              <span class="kv-label">SHA-256 do Arquivo Completo</span>
              <span class="kv-value mono">${m.file_hash || "-"}</span>
            </div>
          ` : ""}
          <div class="kv-row">
            <span class="kv-label">Atualizado em</span>
            <span class="kv-value">${formatDate(m.updated_at)}</span>
          </div>
        </div>
        ${blocksHtml}
      `;
    }

    function openMkdirModal() {
      document.getElementById("mkdirNameInput").value = "";
      document.getElementById("mkdirModal").classList.add("open");
      document.getElementById("mkdirNameInput").focus();
    }

    function openMoveModal(sourcePath) {
      document.getElementById("moveSourceInput").value = sourcePath;
      document.getElementById("moveDestInput").value = sourcePath;
      document.getElementById("moveModal").classList.add("open");
      document.getElementById("moveDestInput").focus();
    }

    function openUploadModal() {
      document.getElementById("uploadFileInput").value = "";
      document.getElementById("uploadTextContent").value = "";
      const prefix = currentPath === "/" ? "" : currentPath;
      document.getElementById("uploadRemotePath").value = `${prefix}/`;
      document.getElementById("uploadModal").classList.add("open");
    }

    function closeModal(id) {
      document.getElementById(id).classList.remove("open");
    }

    function onUploadFileSelected() {
      const fileInput = document.getElementById("uploadFileInput");
      if (fileInput.files && fileInput.files.length > 0) {
        const name = fileInput.files[0].name;
        const prefix = currentPath === "/" ? "" : currentPath;
        document.getElementById("uploadRemotePath").value = `${prefix}/${name}`;
      }
    }

    async function submitMkdir() {
      const name = (document.getElementById("mkdirNameInput").value || "").trim();
      if (!name) return;
      const fullPath = name.startsWith("/")
        ? name
        : (currentPath === "/" ? `/${name}` : `${currentPath}/${name}`);
      try {
        const res = await fetch("/dirs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ path: fullPath, parents: true }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.detail || `HTTP ${res.status}`);
        }
        closeModal("mkdirModal");
        showToast(`Pasta criada: ${fullPath}`);
        await refreshAll();
      } catch (e) {
        showToast(`Erro ao criar pasta: ${e.message}`);
      }
    }

    async function submitMove() {
      const source = document.getElementById("moveSourceInput").value;
      const dest = (document.getElementById("moveDestInput").value || "").trim();
      if (!source || !dest) return;
      try {
        const res = await fetch("/entries/move", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ source_path: source, destination_path: dest }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.detail || `HTTP ${res.status}`);
        }
        closeModal("moveModal");
        showToast(`Movido para ${dest}`);
        selectedEntryPath = dest;
        await refreshAll();
        await selectItem(dest);
      } catch (e) {
        showToast(`Erro ao mover: ${e.message}`);
      }
    }

    async function deleteEntry(path) {
      try {
        const res = await fetch(`/entries?path=${encodeURIComponent(path)}&recursive=true`, {
          method: "DELETE",
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.detail || `HTTP ${res.status}`);
        }
        if (selectedEntryPath === path) {
          selectedEntryPath = null;
          selectedMetadata = null;
        }
        showToast(`Removido: ${path}`);
        await refreshAll();
        renderDetailsDrawer();
      } catch (e) {
        showToast(`Erro ao remover: ${e.message}`);
      }
    }

    async function submitUpload() {
      const fileInput = document.getElementById("uploadFileInput");
      const textContent = document.getElementById("uploadTextContent").value;
      let remotePath = (document.getElementById("uploadRemotePath").value || "").trim();
      const targetN = parseInt(document.getElementById("uploadReplicationTarget").value || "3", 10);
      const encrypt = document.getElementById("uploadEncryptToggle").value === "true";

      let bodyBlob = null;
      if (fileInput.files && fileInput.files.length > 0) {
        bodyBlob = fileInput.files[0];
        if (!remotePath || remotePath.endsWith("/")) {
          remotePath = (currentPath === "/" ? "" : currentPath) + "/" + bodyBlob.name;
        }
      } else if (textContent.length > 0) {
        bodyBlob = new Blob([textContent], { type: "application/octet-stream" });
        if (!remotePath || remotePath.endsWith("/")) {
          remotePath = (currentPath === "/" ? "" : currentPath) + "/nota.txt";
        }
      } else {
        showToast("Selecione um arquivo ou digite um conteúdo de teste.");
        return;
      }

      const btn = document.getElementById("btnSubmitUpload");
      btn.disabled = true;
      btn.textContent = "Enviando blocos...";
      try {
        const url = `/ui/api/upload?path=${encodeURIComponent(remotePath)}&encrypt=${encrypt}&replication_target=${targetN}`;
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/octet-stream" },
          body: bodyBlob,
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(data.detail || `HTTP ${res.status}`);
        }
        closeModal("uploadModal");
        showToast(`Upload concluído: ${data.path} (${data.status})`);
        await refreshAll();
        await selectItem(data.path);
      } catch (e) {
        showToast(`Falha no upload: ${e.message}`);
      } finally {
        btn.disabled = false;
        btn.textContent = "Enviar para o Cluster";
      }
    }

    async function createSampleDemoFile() {
      const stamp = new Date().toISOString().slice(11, 19).replace(/:/g, "");
      const prefix = currentPath === "/" ? "" : currentPath;
      const remotePath = `${prefix}/relatorio_dcomp_${stamp}.txt`;
      const content = `DComp Drive — Relatório Distribuído (${new Date().toISOString()})\n` +
        `Arquivo demonstrativo dividido em blocos com verificação SHA-256 e replicação N=3.\n`.repeat(64);
      try {
        const url = `/ui/api/upload?path=${encodeURIComponent(remotePath)}&encrypt=false&replication_target=3`;
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/octet-stream" },
          body: new Blob([content], { type: "application/octet-stream" }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        showToast(`Arquivo de demonstração criado: ${data.path}`);
        await refreshAll();
        await selectItem(data.path);
      } catch (e) {
        showToast(`Erro ao criar demonstração: ${e.message}`);
      }
    }

    function downloadFile(path) {
      window.location.href = `/ui/api/download?path=${encodeURIComponent(path)}`;
    }

    async function drainNode(nodeId) {
      try {
        const res = await fetch(`/nodes/${encodeURIComponent(nodeId)}/drain`, { method: "POST" });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        showToast(`Nó ${nodeId} em DRAINING (${data.replicas_to_migrate || 0} réplica(s) agendada(s) para migração).`);
        await triggerMonitorCycle();
      } catch (e) {
        showToast(`Erro ao drenar nó: ${e.message}`);
      }
    }

    async function triggerMonitorCycle() {
      try {
        const res = await fetch("/ui/api/monitor-cycle", { method: "POST" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        showToast(
          `Monitor executado: ${data.completed_replications} cópia(s) concluída(s), ` +
          `${(data.newly_suspect || []).length} suspeito(s), ${(data.newly_dead || []).length} morto(s).`
        );
        await refreshAll();
        if (selectedEntryPath) {
          await selectItem(selectedEntryPath);
        }
      } catch (e) {
        showToast(`Erro no ciclo do monitor: ${e.message}`);
      }
    }

    async function refreshAll() {
      await Promise.all([loadDirectory(currentPath), loadClusterStatus()]);
    }

    // Drag and Drop support
    const dropTarget = document.getElementById("dropTarget");
    const dropOverlay = document.getElementById("dropOverlay");

    dropTarget.addEventListener("dragover", (e) => {
      e.preventDefault();
      dropOverlay.classList.add("active");
    });

    dropTarget.addEventListener("dragleave", (e) => {
      if (e.target === dropOverlay) {
        dropOverlay.classList.remove("active");
      }
    });

    dropTarget.addEventListener("drop", async (e) => {
      e.preventDefault();
      dropOverlay.classList.remove("active");
      if (!e.dataTransfer || !e.dataTransfer.files || e.dataTransfer.files.length === 0) return;
      const file = e.dataTransfer.files[0];
      const prefix = currentPath === "/" ? "" : currentPath;
      const remotePath = `${prefix}/${file.name}`;
      showToast(`Enviando ${file.name}...`);
      try {
        const res = await fetch(`/ui/api/upload?path=${encodeURIComponent(remotePath)}&encrypt=false&replication_target=3`, {
          method: "POST",
          headers: { "Content-Type": "application/octet-stream" },
          body: file,
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
        showToast(`Upload concluído: ${data.path}`);
        await refreshAll();
        await selectItem(data.path);
      } catch (err) {
        showToast(`Erro no upload: ${err.message}`);
      }
    });

    // Inicialização
    syncDrawerVisibility();
    renderBreadcrumbs();
    refreshAll();
    setInterval(() => {
      if (activeTab === "drive") {
        loadClusterStatus();
      }
    }, 5000);
  </script>
</body>
</html>
"""


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
@router.get("/drive", response_class=HTMLResponse, include_in_schema=False)
async def serve_drive_ui() -> HTMLResponse:
    """Serve a interface web estilo Google Drive do DComp Drive."""
    return HTMLResponse(content=DRIVE_HTML)
