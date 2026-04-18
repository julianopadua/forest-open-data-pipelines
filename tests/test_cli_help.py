# tests/test_cli_help.py
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _run_help(args: list[str]) -> str:
    repo = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-m", "forest_pipelines.cli", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_root_help_lists_commands_and_dataset_ids() -> None:
    out = _run_help(["--help"])
    assert "anp-catalog" in out
    assert "anp-compact" in out
    assert "anp-publish" in out
    assert "sync" in out
    assert "build-report" in out
    assert "audit-dataset" in out
    assert "eia_petroleum_weekly" in out
    assert "SUPABASE_URL" in out
    assert "bdqueimadas_overview" in out


def test_sync_help_includes_examples() -> None:
    out = _run_help(["sync", "--help"])
    assert "manifest.json" in out
    assert "forest-pipelines sync eia_petroleum_weekly" in out
