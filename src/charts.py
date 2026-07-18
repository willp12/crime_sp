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

# Minimo de BOs no ano anterior para um bairro entrar na comparacao anual
# (evita variacoes percentuais explosivas em bairros com pouquissimos BOs)
MIN_BOS_COMPARACAO = 30


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


def serie_comparativa(df: pl.DataFrame, ano_a: int, ano_b: int) -> px.line:
    """Line chart mensal sobreposto de dois anos (ano base em destaque).

    Espera o resultado de get_comparativo_mensal (colunas ANO, MES, total).
    """
    mensal = (
        df.group_by(["ANO", "MES"])
        .agg(pl.col("total").sum())
        .sort(["ANO", "MES"])
        .with_columns(pl.col("ANO").cast(pl.String))
        .to_pandas()
    )
    fig = px.line(
        mensal,
        x="MES",
        y="total",
        color="ANO",
        markers=True,
        labels={"total": "Total de BOs", "MES": "Mês", "ANO": "Ano"},
        color_discrete_map={str(ano_a): "#E8593C", str(ano_b): "#999999"},
        category_orders={"ANO": [str(ano_b), str(ano_a)]},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
        xaxis=dict(tickmode="linear", dtick=1),
        xaxis_title="Mês",
        yaxis_title="Total de BOs",
    )
    return fig


def variacao_bairros(df: pl.DataFrame, n: int = 15) -> px.bar:
    """Barra horizontal divergente da variacao de BOs por bairro entre dois anos.

    Espera o resultado de get_comparativo_bairros (BAIRRO, total_atual,
    total_anterior). Considera apenas bairros com pelo menos
    MIN_BOS_COMPARACAO BOs no ano anterior.
    """
    top = (
        df.filter(pl.col("total_anterior") >= MIN_BOS_COMPARACAO)
        .with_columns(
            (pl.col("total_atual") - pl.col("total_anterior")).alias("delta")
        )
        .with_columns(
            (pl.col("delta") / pl.col("total_anterior") * 100).round(1).alias("delta_pct"),
            pl.when(pl.col("delta") >= 0)
            .then(pl.lit("Alta"))
            .otherwise(pl.lit("Queda"))
            .alias("direcao"),
        )
        .sort(pl.col("delta").abs(), descending=True)
        .head(n)
        .sort("delta")
        .to_pandas()
    )
    fig = px.bar(
        top,
        x="delta",
        y="BAIRRO",
        orientation="h",
        color="direcao",
        color_discrete_map={"Alta": "#E8593C", "Queda": "#2A9D8F"},
        labels={"delta": "Variação de BOs", "BAIRRO": "Bairro", "direcao": ""},
        hover_data={"delta_pct": True, "total_atual": True, "total_anterior": True},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=500,
        yaxis_title=None,
        xaxis_title="Variação de BOs (ano base − ano comparado)",
        legend_title_text=None,
    )
    return fig


def comparativo_tipos(df: pl.DataFrame) -> px.bar:
    """Barras agrupadas por tipo de crime (RUBRICA_MOD) nos dois anos.

    Espera o resultado de get_comparativo_mensal (ANO, RUBRICA_MOD, total).
    """
    dist = (
        df.group_by(["ANO", "RUBRICA_MOD"])
        .agg(pl.col("total").sum())
        .sort(["ANO", "RUBRICA_MOD"])
        .with_columns(pl.col("ANO").cast(pl.String))
        .to_pandas()
    )
    fig = px.bar(
        dist,
        x="ANO",
        y="total",
        color="RUBRICA_MOD",
        barmode="group",
        labels={"total": "Total de BOs", "ANO": "Ano", "RUBRICA_MOD": "Tipo"},
        color_discrete_map=COLORS,
        category_orders={"RUBRICA_MOD": ["Roubo", "Furto", "Perda", "Outros"]},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
        xaxis_title=None,
        yaxis_title="Total de BOs",
        legend_title_text="Tipo",
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
