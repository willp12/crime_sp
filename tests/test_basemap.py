"""Testes da camada de tiles do mapa base (src/basemap.py)."""

from __future__ import annotations

import pytest

from src.basemap import CARTO_POSITRON_URL, criar_tile_layer

# Host antigo, sem key: hoje so devolve tiles com a marca d'agua do CARTO
HOST_ANTIGO = "{s}.basemaps.cartocdn.com"


def test_com_key_usa_carto_positron_com_key_na_url() -> None:
    layer = criar_tile_layer("abc123")

    assert layer.tiles == f"{CARTO_POSITRON_URL}?key=abc123"
    assert layer.tiles.startswith(
        "https://basemaps.cartocdn.com/rastertiles/light_all/{z}/{x}/{y}.png"
    )


def test_com_key_exibe_atribuicao_do_carto_e_do_osm() -> None:
    attr = criar_tile_layer("abc123").options["attribution"]

    assert "CARTO" in attr
    assert "OpenStreetMap" in attr


def test_key_e_escapada_e_sem_espacos_nas_pontas() -> None:
    layer = criar_tile_layer("  a b/c&d  ")

    assert layer.tiles.endswith("?key=a%20b%2Fc%26d")


@pytest.mark.parametrize("api_key", [None, "", "   "])
def test_sem_key_cai_no_openstreetmap(api_key: str | None) -> None:
    layer = criar_tile_layer(api_key)

    assert layer.tiles.startswith("https://tile.openstreetmap.org/")
    assert "cartocdn" not in layer.tiles


@pytest.mark.parametrize("api_key", [None, "abc123"])
def test_nunca_usa_o_host_antigo_sem_key(api_key: str | None) -> None:
    assert HOST_ANTIGO not in criar_tile_layer(api_key).tiles
