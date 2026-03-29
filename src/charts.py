"""Funcoes de criacao de graficos Plotly para o dashboard."""

from __future__ import annotations

import plotly.express as px
import polars as pl

# Palette consistente para o app
COLORS = {
    "Roubo": "#E8593C",
    "Furto": "#F4A261",
    "Perda": "#2A9D8F",
    "Outros": "#264653",
}

PERIOD_ORDER = ["De madrugada", "Pela manhã", "A tarde", "A noite", "Em hora incerta"]

PERIOD_COLORS = {
    "De madrugada": "#264653",
    "Pela manhã": "#F4A261",
    "A tarde": "#E8593C",
    "A noite": "#2A9D8F",
    "Em hora incerta": "#999999",
}


def top_bairros(df: pl.DataFrame, n: int = 10) -> px.bar:
    """Bar chart horizontal dos top N bairros por numero de BOs."""
    top = (
        df.filter(pl.col("BAIRRO").is_not_null() & (pl.col("BAIRRO") != ""))
        .group_by("BAIRRO")
        .len()
        .sort("len", descending=True)
        .head(n)
        .sort("len")
        .to_pandas()
    )
    fig = px.bar(
        top,
        x="len",
        y="BAIRRO",
        orientation="h",
        labels={"len": "Total de BOs", "BAIRRO": "Bairro"},
        color_discrete_sequence=["#E8593C"],
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
        yaxis_title=None,
        xaxis_title="Total de BOs",
    )
    return fig


def distribuicao_periodo(df: pl.DataFrame) -> px.pie:
    """Donut chart da distribuicao por periodo do dia."""
    dist = (
        df.filter(
            pl.col("DESCR_PERIODO").is_not_null()
            & (pl.col("DESCR_PERIODO") != "NULL")
            & (pl.col("DESCR_PERIODO") != "")
        )
        .group_by("DESCR_PERIODO")
        .len()
        .sort("len", descending=True)
        .to_pandas()
    )
    fig = px.pie(
        dist,
        values="len",
        names="DESCR_PERIODO",
        hole=0.4,
        color="DESCR_PERIODO",
        color_discrete_map=PERIOD_COLORS,
        category_orders={"DESCR_PERIODO": PERIOD_ORDER},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
        legend_title_text="Período",
    )
    fig.update_traces(textinfo="percent+label", textposition="outside")
    return fig


def tipos_crime(df: pl.DataFrame) -> px.pie:
    """Donut chart dos tipos de crime (RUBRICA_MOD)."""
    dist = (
        df.group_by("RUBRICA_MOD")
        .len()
        .sort("len", descending=True)
        .to_pandas()
    )
    fig = px.pie(
        dist,
        values="len",
        names="RUBRICA_MOD",
        hole=0.4,
        color="RUBRICA_MOD",
        color_discrete_map=COLORS,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
        legend_title_text="Tipo",
    )
    fig.update_traces(textinfo="percent+label", textposition="outside")
    return fig


def serie_temporal(df: pl.DataFrame) -> px.line:
    """Line chart de BOs por mes."""
    mensal = (
        df.group_by(["ANO", "MES"])
        .len()
        .sort(["ANO", "MES"])
        .with_columns(
            pl.format("{}-{}", pl.col("ANO"), pl.col("MES").cast(pl.String).str.zfill(2))
            .alias("MES_ANO")
        )
        .to_pandas()
    )
    fig = px.line(
        mensal,
        x="MES_ANO",
        y="len",
        labels={"len": "Total de BOs", "MES_ANO": "Mês"},
        markers=True,
        color_discrete_sequence=["#E8593C"],
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
        xaxis_title=None,
        yaxis_title="Total de BOs",
    )
    return fig


def top_marcas(df: pl.DataFrame, n: int = 10) -> px.bar:
    """Bar chart horizontal das top N marcas de celular."""
    top = (
        df.filter(
            pl.col("MARCA_OBJETO").is_not_null()
            & (pl.col("MARCA_OBJETO") != "")
        )
        .group_by("MARCA_OBJETO")
        .len()
        .sort("len", descending=True)
        .head(n)
        .sort("len")
        .to_pandas()
    )
    fig = px.bar(
        top,
        x="len",
        y="MARCA_OBJETO",
        orientation="h",
        labels={"len": "Total de BOs", "MARCA_OBJETO": "Marca"},
        color_discrete_sequence=["#2A9D8F"],
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
        yaxis_title=None,
        xaxis_title="Total de BOs",
    )
    return fig
