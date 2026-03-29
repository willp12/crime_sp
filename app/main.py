"""Pagina principal — Mapa de celulares subtraidos em Sao Paulo."""

from __future__ import annotations

import sys
from pathlib import Path

import folium
import streamlit as st
from folium.plugins import FastMarkerCluster
from streamlit_folium import st_folium

# Adicionar raiz do projeto ao path para imports de src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data import get_cobertura, get_filter_options, get_mapa_data  # noqa: E402

st.set_page_config(page_title="Crime SP", page_icon="🗺️", layout="wide")

st.title("🗺️ Mapa de Celulares Subtraídos — São Paulo")

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
    tipos_crime = st.multiselect("Tipo de crime", options=options["tipos_crime"])
    periodos = st.multiselect("Período do dia", options=options["periodos"])

# --- Dados ---
with st.spinner("Carregando dados do mapa..."):
    df_mapa = get_mapa_data(
        ano=ano,
        trimestres=trimestres,
        bairros=bairros or None,
        tipos_crime=tipos_crime or None,
        periodos=periodos or None,
    )
    cobertura = get_cobertura(ano=ano, trimestres=trimestres)

# --- Nota de cobertura ---
total = cobertura["total"]
com_coords = cobertura["com_coordenadas"]
pct = (com_coords / total * 100) if total > 0 else 0
st.caption(
    f"📍 **{com_coords:,}** de **{total:,}** BOs possuem geolocalização "
    f"e estão no mapa ({pct:.1f}%)"
)

# --- Metricas rapidas ---
col1, col2, col3 = st.columns(3)
col1.metric("BOs no mapa", f"{df_mapa.shape[0]:,}")
col2.metric("Total de BOs (cidade)", f"{total:,}")
col3.metric("Cobertura geo", f"{pct:.1f}%")

# --- Mapa ---
if df_mapa.shape[0] == 0:
    st.info("Nenhum BO com coordenadas para os filtros selecionados.")
else:
    mapa = folium.Map(
        location=[-23.5505, -46.6333],
        zoom_start=11,
        tiles="CartoDB positron",
    )

    coords = df_mapa.select("LATITUDE", "LONGITUDE").to_numpy().tolist()
    FastMarkerCluster(data=coords).add_to(mapa)

    st_folium(mapa, width=None, height=600, returned_objects=[])
