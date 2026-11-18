"""Valida parsing e mensagens da CLI sem depender dos servidores."""

import pytest
from typer.testing import CliRunner

from src.client.cli import app


@pytest.mark.parametrize(
    "command",
    ["put", "get", "rm", "mv", "ls", "mkdir", "stat", "cluster-status", "drain"],
)
def test_commands_have_help(command):
    result = CliRunner().invoke(app, [command, "--help"])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    "arguments",
    [
        ["--concurrency", "0", "ls"],
        ["--timeout", "0", "ls"],
        ["--node-url", "invalid", "ls"],
        ["put", "missing-local-file", "/remote"],
    ],
)
def test_cli_rejects_invalid_options(arguments):
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code != 0


def test_ls_accepts_optional_positional_path(monkeypatch):
    seen = []

    async def fake_ls(self, path="/"):
        seen.append(path)
        return {"path": path, "entries": []}

    monkeypatch.setattr("src.client.transfer.DriveClient.ls", fake_ls)
    runner = CliRunner()
    for arguments in (["--json", "ls"], ["--json", "ls", "/dados"]):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, result.output
    assert seen == ["/", "/dados"]
