"""Testes do manifest gerado pelo upload (sem rede)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.upload import build_manifest


def _escrever_particao(base: Path, ano: int, trimestre: int, linhas: int) -> None:
    part_dir = base / f"ano={ano}" / f"trimestre={trimestre}"
    part_dir.mkdir(parents=True)
    pl.DataFrame({"NUM_BO": [f"X{i}" for i in range(linhas)]}).write_parquet(
        part_dir / "data.parquet"
    )


def test_build_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("PIPELINE_SOURCE", raising=False)

    transformed = tmp_path / "transformed" / "parquet"
    _escrever_particao(transformed, 2026, 1, 3)
    _escrever_particao(transformed, 2026, 2, 5)

    manifest = build_manifest(transformed)

    assert manifest["schema_version"] == 1
    assert manifest["source"] == "local"
    assert manifest["anos_processados"] == [2026]
    assert manifest["total_rows"] == 8
    assert manifest["partitions"] == [
        {"ano": 2026, "trimestre": 1, "rows": 3},
        {"ano": 2026, "trimestre": 2, "rows": 5},
    ]
    # processed_at em ISO-8601 UTC, parseavel
    assert datetime.fromisoformat(manifest["processed_at"]).tzinfo is not None


def test_build_manifest_source_github(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    transformed = tmp_path / "transformed" / "parquet"
    _escrever_particao(transformed, 2025, 4, 1)

    assert build_manifest(transformed)["source"] == "github-actions"


def test_build_manifest_source_pipeline_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("PIPELINE_SOURCE", "airflow")

    transformed = tmp_path / "transformed" / "parquet"
    _escrever_particao(transformed, 2025, 4, 1)

    assert build_manifest(transformed)["source"] == "airflow"
