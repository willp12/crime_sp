"""Conexao DuckDB + S3 e funcoes de query para o app Streamlit."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import streamlit as st

S3_TRANSFORMED = "s3://crime-data-sp/transformed/parquet/**/*.parquet"

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


def _read_sql(name: str) -> str:
    """Le arquivo SQL da pasta queries/."""
    path = QUERIES_DIR / name
    return path.read_text(encoding="utf-8")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Cria conexao DuckDB in-memory com httpfs configurado para S3."""
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    aws = st.secrets["aws"]
    conn.execute(f"SET s3_region='{aws['region']}';")
    conn.execute(f"SET s3_access_key_id='{aws['access_key_id']}';")
    conn.execute(f"SET s3_secret_access_key='{aws['secret_access_key']}';")

    return conn


def query_df(sql: str, params: dict | None = None) -> pl.DataFrame:
    """Executa SQL e retorna Polars DataFrame.

    Args:
        sql: Query SQL (pode conter {placeholders} para params).
        params: Dicionario de substituicao para a query.

    Returns:
        Polars DataFrame com os resultados.
    """
    conn = get_connection()
    if params:
        sql = sql.format(**params)
    return conn.execute(sql).pl()


def query_sql_file(name: str, params: dict | None = None) -> pl.DataFrame:
    """Le arquivo .sql e executa, retornando Polars DataFrame."""
    sql = _read_sql(name)
    return query_df(sql, params)


@st.cache_data(ttl=3600)
def get_filter_options() -> dict:
    """Retorna opcoes unicas para os filtros da sidebar."""
    sql = f"""
    SELECT DISTINCT ANO, TRIMESTRE, BAIRRO, RUBRICA_MOD, DESCR_PERIODO, CIDADE
    FROM read_parquet('{S3_TRANSFORMED}', hive_partitioning=true)
    WHERE CIDADE = 'S.PAULO'
    """
    df = query_df(sql)

    return {
        "anos": sorted(df["ANO"].unique().drop_nulls().to_list(), reverse=True),
        "trimestres": sorted(df["TRIMESTRE"].unique().drop_nulls().to_list()),
        "bairros": sorted(df["BAIRRO"].unique().drop_nulls().to_list()),
        "tipos_crime": sorted(df["RUBRICA_MOD"].unique().drop_nulls().to_list()),
        "periodos": sorted(
            [p for p in df["DESCR_PERIODO"].unique().drop_nulls().to_list() if p != "NULL"],
        ),
    }


@st.cache_data(ttl=3600)
def get_mapa_data(
    ano: int,
    trimestres: list[int],
    bairros: list[str] | None = None,
    tipos_crime: list[str] | None = None,
    periodos: list[str] | None = None,
) -> pl.DataFrame:
    """Retorna dados para o mapa (apenas BOs com coordenadas)."""
    sql = _read_sql("mapa.sql")
    where_extra = _build_filters(bairros, tipos_crime, periodos)
    params = {
        "s3_path": S3_TRANSFORMED,
        "ano": ano,
        "trimestres": _sql_list(trimestres),
        "where_extra": where_extra,
    }
    return query_df(sql, params)


@st.cache_data(ttl=3600)
def get_estatisticas_data(
    ano: int,
    trimestres: list[int],
    bairros: list[str] | None = None,
    tipos_crime: list[str] | None = None,
    periodos: list[str] | None = None,
) -> pl.DataFrame:
    """Retorna dados para estatisticas (TODOS os BOs, sem filtro de coordenadas)."""
    sql = _read_sql("estatisticas.sql")
    where_extra = _build_filters(bairros, tipos_crime, periodos)
    params = {
        "s3_path": S3_TRANSFORMED,
        "ano": ano,
        "trimestres": _sql_list(trimestres),
        "where_extra": where_extra,
    }
    return query_df(sql, params)


@st.cache_data(ttl=3600)
def get_cobertura(ano: int, trimestres: list[int]) -> dict:
    """Retorna contagem total e com coordenadas para nota de cobertura."""
    sql = f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN HAS_COORDINATES THEN 1 ELSE 0 END) as com_coordenadas
    FROM read_parquet('{S3_TRANSFORMED}', hive_partitioning=true)
    WHERE CIDADE = 'S.PAULO'
      AND ANO = {ano}
      AND TRIMESTRE IN ({_sql_list(trimestres)})
    """
    df = query_df(sql)
    total = df["total"][0]
    com_coords = df["com_coordenadas"][0]
    return {"total": total, "com_coordenadas": com_coords}


def _build_filters(
    bairros: list[str] | None,
    tipos_crime: list[str] | None,
    periodos: list[str] | None,
) -> str:
    """Constroi clausulas WHERE adicionais a partir dos filtros."""
    parts = []
    if bairros:
        escaped = ", ".join(f"'{b.replace(chr(39), chr(39)*2)}'" for b in bairros)
        parts.append(f"AND BAIRRO IN ({escaped})")
    if tipos_crime:
        escaped = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in tipos_crime)
        parts.append(f"AND RUBRICA_MOD IN ({escaped})")
    if periodos:
        escaped = ", ".join(f"'{p.replace(chr(39), chr(39)*2)}'" for p in periodos)
        parts.append(f"AND DESCR_PERIODO IN ({escaped})")
    return "\n      ".join(parts)


def _sql_list(values: list) -> str:
    """Converte lista Python para lista SQL: 1, 2, 3."""
    return ", ".join(str(v) for v in values)
