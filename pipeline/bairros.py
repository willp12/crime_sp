"""Normalizacao de nomes de bairros para a camada transformed.

Pipeline de normalizacao, aplicado nesta ordem:
1. Trim de espacos nas bordas
2. Uppercase
3. Remocao de acentos (NFKD + remocao de marcas combinantes): "SÃO" -> "SAO"
4. Pontuacao vira espaco: "JD. PAULISTA" -> "JD  PAULISTA"
5. Colapso de espacos multiplos
6. Expansao de abreviacoes comuns, token a token: "JD" -> "JARDIM"
7. Aliases manuais de bairro_aliases.csv (match da string completa, ja
   normalizada pelos passos 1-6)

Nulos propagam intactos e strings vazias permanecem vazias — nenhum valor
e inventado. O valor original e preservado em BAIRRO_ORIGINAL pela camada
transformed (ver transform.py).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

ALIASES_CSV = Path(__file__).resolve().parent / "bairro_aliases.csv"

# Expansao token a token. Lista conservadora: "S" isolado NAO e mapeado
# (ambiguo entre SAO e SANTA) — casos assim vao para bairro_aliases.csv.
ABREVIACOES: dict[str, str] = {
    "JD": "JARDIM",
    "JDM": "JARDIM",
    "VL": "VILA",
    "PQ": "PARQUE",
    "PQE": "PARQUE",
    "STA": "SANTA",
    "STO": "SANTO",
    "CID": "CIDADE",
    "CJ": "CONJUNTO",
    "CONJ": "CONJUNTO",
    "NSA": "NOSSA",
    "SRA": "SENHORA",
    "PCA": "PRACA",
}


def carregar_aliases(csv_path: Path = ALIASES_CSV) -> dict[str, str]:
    """Le o mapa manual de aliases de bairro.

    O CSV tem colunas bairro_variante,bairro_canonico. A variante deve estar
    escrita JA NORMALIZADA pelos passos 1-6 (sem acento, maiuscula, abreviacoes
    expandidas), pois o alias e aplicado por ultimo. Candidatos a alias vem da
    tabela de variantes da pagina de qualidade do app.
    """
    if not csv_path.exists():
        return {}
    df = pl.read_csv(csv_path)
    if df.height == 0:
        return {}
    return dict(zip(df["bairro_variante"].to_list(), df["bairro_canonico"].to_list()))


def normalize_bairro(expr: pl.Expr, aliases: dict[str, str] | None = None) -> pl.Expr:
    """Normaliza nomes de bairros (expressao Polars vetorizada).

    Args:
        expr: Expressao com a coluna de bairro (ex: pl.col("BAIRRO")).
        aliases: Mapa variante normalizada -> nome canonico. Use
            carregar_aliases() para o mapa versionado no repo.

    Returns:
        Expressao com o nome normalizado; nulos e vazios passam intactos.
    """
    normalized = (
        expr.str.strip_chars()
        .str.to_uppercase()
        .str.normalize("NFKD")
        .str.replace_all(r"\p{M}", "")
        .str.replace_all(r"[^A-Z0-9 ]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.split(" ")
        .list.eval(pl.element().replace(ABREVIACOES))
        .list.join(" ")
    )
    if aliases:
        normalized = normalized.replace(aliases)
    return normalized
