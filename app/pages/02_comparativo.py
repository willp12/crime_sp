"""Pagina de comparativo anual — ano base vs ano anterior, com analise por bairro."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.charts import (  # noqa: E402
    MIN_BOS_COMPARACAO,
    comparativo_tipos,
    serie_comparativa,
    variacao_bairros,
)
from src.data import (  # noqa: E402
    get_comparativo_bairros,
    get_comparativo_mensal,
    get_particoes,
)
from src.ui import render_freshness_caption  # noqa: E402

st.set_page_config(page_title="Crime SP — Comparativo", page_icon="🗺️", layout="wide")

st.title("📅 Comparativo Anual")
render_freshness_caption()

# --- Sidebar: selecao dos anos ---
with st.sidebar:
    st.header("Comparação")

    with st.spinner("Carregando períodos..."):
        particoes = get_particoes()

    anos = sorted(particoes["ANO"].unique().drop_nulls().to_list(), reverse=True)

    if len(anos) < 2:
        st.warning("É preciso ao menos dois anos de dados para comparar.")
        st.stop()

    ano_base = st.selectbox("Ano base", anos, index=0)

    anos_restantes = [a for a in anos if a != ano_base]
    idx_default = (
        anos_restantes.index(ano_base - 1) if ano_base - 1 in anos_restantes else 0
    )
    ano_anterior = st.selectbox("Comparar com", anos_restantes, index=idx_default)

# --- Trimestres comparaveis: presentes em ambos os anos ---
tri_base = set(particoes.filter(pl.col("ANO") == ano_base)["TRIMESTRE"].to_list())
tri_ant = set(particoes.filter(pl.col("ANO") == ano_anterior)["TRIMESTRE"].to_list())
trimestres = sorted(tri_base & tri_ant)

if not trimestres:
    st.info("Não há trimestres disponíveis em ambos os anos para comparação.")
    st.stop()

st.caption(
    f"Comparando **{ano_base}** vs **{ano_anterior}** — apenas "
    + ", ".join(f"Q{t}" for t in trimestres)
    + ", trimestre(s) disponível(is) em ambos os anos."
)

# --- Dados ---
with st.spinner("Carregando comparativo..."):
    df_mensal = get_comparativo_mensal(ano_base, ano_anterior, trimestres)
    df_bairros = get_comparativo_bairros(ano_base, ano_anterior, trimestres)

if df_mensal.height == 0:
    st.info("Nenhum BO encontrado para os anos selecionados.")
    st.stop()


def _totais(df: pl.DataFrame, ano: int) -> dict:
    """Totais do ano: geral, roubos, furtos e BOs com coordenadas."""
    sub = df.filter(pl.col("ANO") == ano)
    return {
        "total": int(sub["total"].sum()),
        "roubos": int(sub.filter(pl.col("RUBRICA_MOD") == "Roubo")["total"].sum()),
        "furtos": int(sub.filter(pl.col("RUBRICA_MOD") == "Furto")["total"].sum()),
        "coords": int(sub["com_coordenadas"].sum()),
    }


def _delta_pct(atual: int, anterior: int) -> str | None:
    """Delta percentual formatado, ou None se nao houver base de comparacao."""
    if anterior == 0:
        return None
    return f"{(atual - anterior) / anterior * 100:+.1f}%"


atual = _totais(df_mensal, ano_base)
anterior = _totais(df_mensal, ano_anterior)

pct_geo_atual = atual["coords"] / atual["total"] * 100 if atual["total"] else 0.0
pct_geo_anterior = (
    anterior["coords"] / anterior["total"] * 100 if anterior["total"] else 0.0
)

# delta_color="inverse": queda no numero de crimes aparece em verde
col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "Total de BOs",
    f"{atual['total']:,}",
    delta=_delta_pct(atual["total"], anterior["total"]),
    delta_color="inverse",
)
col2.metric(
    "Roubos",
    f"{atual['roubos']:,}",
    delta=_delta_pct(atual["roubos"], anterior["roubos"]),
    delta_color="inverse",
)
col3.metric(
    "Furtos",
    f"{atual['furtos']:,}",
    delta=_delta_pct(atual["furtos"], anterior["furtos"]),
    delta_color="inverse",
)
col4.metric(
    "Cobertura geo",
    f"{pct_geo_atual:.1f}%",
    delta=f"{pct_geo_atual - pct_geo_anterior:+.1f} p.p.",
)

st.divider()

# --- Serie mensal sobreposta ---
st.subheader(f"BOs por mês — {ano_base} vs {ano_anterior}")
st.plotly_chart(
    serie_comparativa(df_mensal, ano_base, ano_anterior), use_container_width=True
)

# --- Variacao por bairro ---
st.subheader("Maiores variações por bairro")
st.caption(
    f"Bairros com pelo menos {MIN_BOS_COMPARACAO} BOs em {ano_anterior}. "
    "Variação absoluta no número de BOs entre os dois anos."
)
if df_bairros.filter(pl.col("total_anterior") >= MIN_BOS_COMPARACAO).height == 0:
    st.info("Nenhum bairro com volume suficiente para comparação.")
else:
    st.plotly_chart(variacao_bairros(df_bairros), use_container_width=True)

# --- Por tipo de crime ---
st.subheader("Por tipo de crime")
st.plotly_chart(comparativo_tipos(df_mensal), use_container_width=True)

# --- Tabela completa por bairro ---
st.divider()
st.subheader("Tabela completa por bairro")

tabela = (
    df_bairros.with_columns(
        (pl.col("total_atual") - pl.col("total_anterior")).alias("delta")
    )
    .with_columns(
        pl.when(pl.col("total_anterior") > 0)
        .then((pl.col("delta") / pl.col("total_anterior") * 100).round(1))
        .otherwise(None)
        .alias("delta_pct")
    )
    .sort("total_atual", descending=True)
    .to_pandas()
)
tabela.columns = ["Bairro", str(ano_base), str(ano_anterior), "Δ", "Δ%"]
st.dataframe(tabela, use_container_width=True, hide_index=True)
