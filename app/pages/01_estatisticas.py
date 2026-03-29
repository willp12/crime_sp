"""Pagina de estatisticas — Dashboard com graficos Plotly."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.charts import (  # noqa: E402
    distribuicao_periodo,
    serie_temporal,
    tipos_crime,
    top_bairros,
    top_marcas,
)
from src.data import get_cobertura, get_estatisticas_data, get_filter_options  # noqa: E402

st.set_page_config(page_title="Crime SP — Estatísticas", page_icon="🗺️", layout="wide")

st.title("📊 Estatísticas — Celulares Subtraídos")

# --- Sidebar: filtros globais ---
with st.sidebar:
    st.header("Filtros")

    with st.spinner("Carregando filtros..."):
        options = get_filter_options()

    ano = st.selectbox("Ano", options["anos"], index=0)

    trimestres = st.multiselect(
        "Trimestre",
        options=options["trimestres"],
        default=options["trimestres"],
        format_func=lambda t: f"Q{t}",
    )

    if not trimestres:
        st.warning("Selecione ao menos um trimestre.")
        st.stop()

    bairros = st.multiselect("Bairro", options=options["bairros"])
    tipos_crime_filter = st.multiselect("Tipo de crime", options=options["tipos_crime"])
    periodos = st.multiselect("Período do dia", options=options["periodos"])

# --- Dados ---
with st.spinner("Carregando estatísticas..."):
    df = get_estatisticas_data(
        ano=ano,
        trimestres=trimestres,
        bairros=bairros or None,
        tipos_crime=tipos_crime_filter or None,
        periodos=periodos or None,
    )
    cobertura = get_cobertura(ano=ano, trimestres=trimestres)

# --- Metricas no topo ---
total_bos = df.shape[0]
total_cidade = cobertura["total"]
com_coords = cobertura["com_coordenadas"]
pct_geo = (com_coords / total_cidade * 100) if total_cidade > 0 else 0

# Bairro mais critico
bairro_top = (
    df.filter(df["BAIRRO"].is_not_null() & (df["BAIRRO"] != ""))
    .group_by("BAIRRO")
    .len()
    .sort("len", descending=True)
    .head(1)
)
bairro_nome = bairro_top["BAIRRO"][0] if bairro_top.shape[0] > 0 else "—"

# Periodo mais critico
periodo_top = (
    df.filter(df["DESCR_PERIODO"].is_not_null() & (df["DESCR_PERIODO"] != "NULL"))
    .group_by("DESCR_PERIODO")
    .len()
    .sort("len", descending=True)
    .head(1)
)
periodo_nome = periodo_top["DESCR_PERIODO"][0] if periodo_top.shape[0] > 0 else "—"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de BOs", f"{total_bos:,}")
col2.metric("Bairro mais crítico", bairro_nome)
col3.metric("Período mais crítico", periodo_nome)
col4.metric("Cobertura geo", f"{pct_geo:.1f}%")

st.divider()

if total_bos == 0:
    st.info("Nenhum BO encontrado para os filtros selecionados.")
    st.stop()

# --- Graficos ---
tab1, tab2, tab3, tab4 = st.tabs([
    "🏘️ Por Bairro",
    "🕐 Por Período",
    "📱 Por Tipo / Marca",
    "📈 Série Temporal",
])

with tab1:
    st.subheader("Top 10 bairros com mais BOs")
    st.plotly_chart(top_bairros(df), use_container_width=True)

with tab2:
    st.subheader("Distribuição por período do dia")
    st.plotly_chart(distribuicao_periodo(df), use_container_width=True)

with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Tipos de crime")
        st.plotly_chart(tipos_crime(df), use_container_width=True)
    with c2:
        st.subheader("Top 10 marcas de celular")
        st.plotly_chart(top_marcas(df), use_container_width=True)

with tab4:
    st.subheader("BOs por mês")
    st.plotly_chart(serie_temporal(df), use_container_width=True)

# --- Tabela detalhada ---
st.divider()
st.subheader("Tabela detalhada")
st.caption(f"Mostrando até 1.000 registros dos {total_bos:,} BOs filtrados.")

display_df = (
    df.select([
        "NUM_BO", "DATA_OCORRENCIA_BO", "DESCR_PERIODO",
        "RUBRICA_MOD", "BAIRRO", "LOGRADOURO", "MARCA_OBJETO",
    ])
    .sort("DATA_OCORRENCIA_BO", descending=True)
    .head(1000)
    .to_pandas()
)
display_df.columns = ["BO", "Data", "Período", "Tipo", "Bairro", "Logradouro", "Marca"]
st.dataframe(display_df, use_container_width=True, hide_index=True)
