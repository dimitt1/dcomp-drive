"""CLI Typer/Rich. Execute: python -m src.client --help."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from src.client.transfer import ClientError, DriveClient
from src.common.config import get_settings

app = typer.Typer(no_args_is_help=True, help="DComp Drive — arquivos distribuídos.")
console = Console(highlight=False)
errors = Console(stderr=True, highlight=False)


@dataclass
class Options:
    controller_url: str | None
    concurrency: int
    timeout: float
    json_output: bool
    node_urls: dict[str, str]


@app.callback()
def main(
    ctx: typer.Context,
    controller_url: Annotated[
        str | None,
        typer.Option(
            help="URL do controlador; fora do Docker use http://localhost:8000."
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(exists=True, dir_okay=False, help="Configuração YAML."),
    ] = None,
    concurrency: Annotated[
        int, typer.Option(min=1, help="Limite de transferências simultâneas.")
    ] = 4,
    timeout: Annotated[
        float, typer.Option(min=0.1, help="Timeout HTTP em segundos.")
    ] = 30.0,
    json_output: Annotated[
        bool, typer.Option("--json", help="Saída JSON para automação.")
    ] = False,
    node_url: Annotated[
        list[str] | None,
        typer.Option(help="Mapeamento opcional nó=URL acessível, repetível."),
    ] = None,
):
    if config is not None:
        os.environ["CONFIG_PATH"] = str(config.resolve())
        get_settings.cache_clear()
    overrides = {}
    for value in node_url or []:
        node, separator, url = value.partition("=")
        if not separator or not node or not url:
            raise typer.BadParameter("Use --node-url nó=http://host:porta.")
        overrides[node] = url
    ctx.obj = Options(controller_url, concurrency, timeout, json_output, overrides)


def run(ctx: typer.Context, operation: str, *args, **kwargs):
    options: Options = ctx.obj

    async def execute():
        async with DriveClient(
            controller_url=options.controller_url,
            concurrency=options.concurrency,
            timeout=options.timeout,
            node_urls=options.node_urls,
        ) as client:
            try:
                return await getattr(client, operation)(*args, **kwargs)
            finally:
                for warning in client.warnings:
                    errors.print(f"Aviso: {warning}", markup=False)

    try:
        result = asyncio.run(execute())
    except (ClientError, OSError, ValueError) as exc:
        errors.print(f"Erro: {exc}", markup=False)
        raise typer.Exit(1) from exc
    if options.json_output:
        payload = (
            result.model_dump(mode="json") if isinstance(result, BaseModel) else result
        )
        typer.echo(json.dumps(payload, ensure_ascii=False))
    return result


@app.command()
def put(
    ctx: typer.Context,
    local: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
    remote: str,
    encrypt: Annotated[
        bool | None,
        typer.Option(
            "--encrypt/--no-encrypt", help="Usa a configuração quando omitido."
        ),
    ] = None,
    replicas: Annotated[
        int | None, typer.Option(min=1, help="Fator de replicação N.")
    ] = None,
):
    result = run(
        ctx, "put", local, remote, encrypt=encrypt, replication_target=replicas
    )
    if not ctx.obj.json_output:
        console.print(
            f"{result.path}: {result.status.value} ({result.total_blocks} blocos, W={result.write_quorum_used})",
            markup=False,
        )


@app.command()
def get(ctx: typer.Context, remote: str, local: Path):
    result = run(ctx, "get", remote, local)
    if not ctx.obj.json_output:
        console.print(
            f"Salvo em {local} ({result.size_bytes} bytes); SHA-256 verificado.",
            markup=False,
        )


@app.command()
def mkdir(
    ctx: typer.Context,
    path: str,
    parents: Annotated[bool, typer.Option("--parents/--no-parents", "-p")] = True,
):
    result = run(ctx, "mkdir", path, parents=parents)
    if not ctx.obj.json_output:
        console.print(f"Diretório: {result.path}", markup=False)


@app.command()
def ls(ctx: typer.Context, path: Annotated[str, typer.Argument()] = "/"):
    result = run(ctx, "ls", path)
    if not ctx.obj.json_output:
        table = Table(title=result.path)
        for column in ("Nome", "Tipo", "Bytes", "Estado", "Cifrado"):
            table.add_column(column)
        for item in result.entries:
            table.add_row(
                item.name,
                "diretório" if item.is_dir else "arquivo",
                str(item.size_bytes),
                item.status.value,
                "sim" if item.is_encrypted else "não",
            )
        console.print(table)


@app.command()
def rm(
    ctx: typer.Context,
    path: str,
    recursive: Annotated[
        bool, typer.Option("--recursive", "-r", help="Remove diretórios não vazios.")
    ] = False,
):
    result = run(ctx, "rm", path, recursive=recursive)
    if not ctx.obj.json_output:
        console.print(
            f"Removido: {result.deleted_path} ({result.deleted_entries_count} entradas)",
            markup=False,
        )


@app.command()
def mv(ctx: typer.Context, source: str, destination: str):
    result = run(ctx, "mv", source, destination)
    if not ctx.obj.json_output:
        console.print(f"Movido para: {result.path}", markup=False)


@app.command()
def stat(ctx: typer.Context, path: str):
    result = run(ctx, "stat", path)
    if not ctx.obj.json_output:
        console.print(
            f"{result.path}\nEstado: {result.status.value}\nBytes: {result.size_bytes}\nSHA-256: {result.file_hash or '-'}\nCifrado: {result.is_encrypted}\nReplicação N: {result.replication_target}\nLease: {result.lease_expires_at or '-'}",
            markup=False,
        )
        table = Table(title="Blocos e réplicas")
        for column in (
            "Índice",
            "Bloco / SHA-256",
            "Bytes",
            "Nó",
            "Disco",
            "Réplica",
            "Estado do nó",
        ):
            table.add_column(column)
        for block in result.blocks:
            for replica in block.replicas:
                table.add_row(
                    str(block.block_index),
                    f"{block.block_id}\n{block.block_hash}",
                    str(block.size_bytes),
                    replica.node_id,
                    replica.disk_id,
                    replica.status.value,
                    replica.node_status.value,
                )
            if not block.replicas:
                table.add_row(
                    str(block.block_index),
                    f"{block.block_id}\n{block.block_hash}",
                    str(block.size_bytes),
                    "-",
                    "-",
                    "sem réplicas",
                    "-",
                )
        console.print(table)


@app.command("cluster-status")
def cluster_status(ctx: typer.Context):
    result = run(ctx, "cluster_status")
    if not ctx.obj.json_output:
        table = Table(title="Cluster")
        for column in (
            "Nó",
            "Máquina",
            "Estado",
            "Disco",
            "Total (B)",
            "Usado (B)",
            "Livre (B)",
            "Uso %",
            "Ativo",
        ):
            table.add_column(column)
        for node in result["nodes"]:
            for disk in node["disks"]:
                table.add_row(
                    node["node_id"],
                    node["machine_id"],
                    node["status"],
                    disk["disk_id"],
                    str(disk["total_bytes"]),
                    str(disk["used_bytes"]),
                    str(disk["free_bytes"]),
                    str(disk["usage_pct"]),
                    "sim" if disk["is_active"] else "não",
                )
            if not node["disks"]:
                table.add_row(
                    node["node_id"], node["machine_id"], node["status"], "sem discos"
                )
        console.print(table)
        console.print(f"Re-replicações ativas: {result['active_re_replications']}")
        for status, count in result["files_by_status"].items():
            console.print(f"{status}: {count}", markup=False)


@app.command()
def drain(ctx: typer.Context, node_id: str):
    result = run(ctx, "drain", node_id)
    if not ctx.obj.json_output:
        console.print(f"{result['node_id']}: {result['status']}", markup=False)


if __name__ == "__main__":
    app()
