"""Testes da normalizacao de nomes de bairros."""

from __future__ import annotations

import polars as pl
import pytest

from pipeline.bairros import carregar_aliases, normalize_bairro


def _normalizar(valor: str | None, aliases: dict[str, str] | None = None) -> str | None:
    """Aplica normalize_bairro a um unico valor e retorna o resultado."""
    df = pl.DataFrame({"BAIRRO": [valor]}, schema={"BAIRRO": pl.String})
    return df.with_columns(
        normalize_bairro(pl.col("BAIRRO"), aliases).alias("OUT")
    )["OUT"][0]


@pytest.mark.parametrize(
    ("bruto", "esperado"),
    [
        ("  jd  paulista ", "JARDIM PAULISTA"),
        ("VL. MARIANA", "VILA MARIANA"),
        ("SÃO JOÃO CLÍMACO", "SAO JOAO CLIMACO"),
        ("PQ. DO CARMO", "PARQUE DO CARMO"),
        ("STA CECILIA", "SANTA CECILIA"),
        ("STO AMARO", "SANTO AMARO"),
        ("JDM ANGELA", "JARDIM ANGELA"),
        ("CJ HAB TAIPAS", "CONJUNTO HAB TAIPAS"),
        ("CENTRO", "CENTRO"),
        (None, None),
        ("", ""),
    ],
)
def test_normalize_bairro(bruto: str | None, esperado: str | None) -> None:
    assert _normalizar(bruto) == esperado


def test_alias_aplicado_apos_normalizacao() -> None:
    # A chave do alias e a string JA normalizada pelos passos anteriores
    aliases = {"JARDIM PAULISTA": "JARDIM PAULISTA UNIFICADO"}
    assert _normalizar(" jd. paulista", aliases) == "JARDIM PAULISTA UNIFICADO"


def test_alias_sem_match_nao_altera() -> None:
    assert _normalizar("MOEMA", {"OUTRO BAIRRO": "X"}) == "MOEMA"


def test_carregar_aliases_arquivo_inexistente(tmp_path) -> None:
    assert carregar_aliases(tmp_path / "nao_existe.csv") == {}


def test_carregar_aliases_apenas_cabecalho(tmp_path) -> None:
    csv = tmp_path / "aliases.csv"
    csv.write_text("bairro_variante,bairro_canonico\n", encoding="utf-8")
    assert carregar_aliases(csv) == {}


def test_carregar_aliases_csv(tmp_path) -> None:
    csv = tmp_path / "aliases.csv"
    csv.write_text(
        "bairro_variante,bairro_canonico\nBELA VISTA CENTRO,BELA VISTA\n",
        encoding="utf-8",
    )
    assert carregar_aliases(csv) == {"BELA VISTA CENTRO": "BELA VISTA"}
