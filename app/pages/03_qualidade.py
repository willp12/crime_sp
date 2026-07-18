"""Pagina de qualidade dos dados — cobertura, completude e transformacoes."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.data import (  # noqa: E402
    get_manifest,
    get_particoes,
    get_qualidade,
    get_variantes_bairro,
)
from src.ui import render_freshness_caption  # noqa: E402

st.set_page_config(page_title="Crime SP — Qualidade", page_icon="🗺️", layout="wide")

st.title("🔬 Qualidade dos Dados")
render_freshness_caption()

st.markdown(
    "Esta página documenta as transformações aplicadas aos dados e expõe as "
    "limitações conhecidas do dataset. Princípio do projeto: **nenhum registro "
    "é excluído** em nenhuma camada — problemas de qualidade são medidos e "
    "informados, não escondidos."
)

# --- 1. Ultimo processamento e particoes ---
st.subheader("📦 Partições e cobertura de geolocalização")

manifest = get_manifest()
if manifest:
    origem = {
        "github-actions": "GitHub Actions",
        "airflow": "Airflow",
        "local": "execução local",
    }.get(manifest.get("source", ""), manifest.get("source", "desconhecida"))
    try:
        processado = datetime.fromisoformat(manifest["processed_at"])
        quando = processado.strftime("%d/%m/%Y %H:%M UTC")
    except (KeyError, ValueError):
        quando = "data desconhecida"
    st.caption(
        f"Último run do pipeline: **{quando}** via **{origem}** — "
        f"{manifest.get('total_rows', 0):,} linhas em "
        f"{len(manifest.get('partitions', []))} partição(ões) processada(s) no run."
    )
else:
    st.caption(
        "Manifest de processamento ainda não disponível — será gravado no "
        "próximo run do pipeline."
    )

with st.spinner("Carregando partições..."):
    particoes = get_particoes()

part_display = particoes.with_columns(
    (pl.col("com_coordenadas") / pl.col("total") * 100).round(1).alias("pct_geo")
).to_pandas()

st.dataframe(
    part_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "ANO": st.column_config.NumberColumn("Ano", format="%d"),
        "TRIMESTRE": st.column_config.NumberColumn("Trimestre", format="Q%d"),
        "total": st.column_config.NumberColumn("Total de BOs"),
        "com_coordenadas": st.column_config.NumberColumn("Com coordenadas"),
        "pct_geo": st.column_config.ProgressColumn(
            "Cobertura geo", min_value=0, max_value=100, format="%.1f%%"
        ),
    },
)
st.caption(
    "BOs sem coordenadas permanecem no dataset e entram em todas as "
    "estatísticas; apenas o mapa filtra por `HAS_COORDINATES = true`."
)

# --- 2. Completude por trimestre ---
st.subheader("✅ Completude por trimestre")

qualidade = get_qualidade()
if qualidade is None:
    st.info(
        "Métricas de completude ficam disponíveis após o reprocessamento dos "
        "dados históricos (backfill) com o pipeline atualizado."
    )
else:
    qual_display = (
        qualidade.with_columns(
            (pl.col("linhas") - pl.col("bos_unicos")).alias("linhas_excedentes"),
            (pl.col("bairro_vazio") / pl.col("linhas") * 100).round(1).alias("pct_bairro_vazio"),
            (pl.col("periodo_nulo") / pl.col("linhas") * 100).round(1).alias("pct_periodo_nulo"),
            (pl.col("marca_vazia") / pl.col("linhas") * 100).round(1).alias("pct_marca_vazia"),
        )
        .select(
            "ANO",
            "TRIMESTRE",
            "linhas",
            "bos_unicos",
            "linhas_excedentes",
            "pct_bairro_vazio",
            "pct_periodo_nulo",
            "pct_marca_vazia",
        )
        .to_pandas()
    )
    st.dataframe(
        qual_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ANO": st.column_config.NumberColumn("Ano", format="%d"),
            "TRIMESTRE": st.column_config.NumberColumn("Trimestre", format="Q%d"),
            "linhas": st.column_config.NumberColumn("Linhas"),
            "bos_unicos": st.column_config.NumberColumn("BOs únicos"),
            "linhas_excedentes": st.column_config.NumberColumn("Linhas excedentes (versões)"),
            "pct_bairro_vazio": st.column_config.NumberColumn("% bairro vazio", format="%.1f%%"),
            "pct_periodo_nulo": st.column_config.NumberColumn("% período nulo", format="%.1f%%"),
            "pct_marca_vazia": st.column_config.NumberColumn("% marca vazia", format="%.1f%%"),
        },
    )
    st.caption(
        "**Linhas excedentes (versões):** um mesmo BO pode aparecer em múltiplas "
        "versões (coluna `VERSAO` da fonte SSP). As contagens do app somam todas "
        "as linhas — este número mede a inflação potencial das contagens. "
        "**% período nulo** conta registros com `DESCR_PERIODO` ausente ou com o "
        "literal `\"NULL\"` vindo da fonte."
    )

# --- 3. Normalizacao de bairros ---
st.subheader("🏘️ Normalização de bairros")

st.markdown(
    "Os nomes de bairros vêm da fonte com variações de grafia (acentos, "
    "abreviações como `JD`/`VL`/`PQ`, pontuação e espaçamento). O pipeline "
    "unifica essas variantes em um nome canônico — a tabela abaixo mostra o "
    "que foi unificado, com o valor original preservado em `BAIRRO_ORIGINAL`."
)

variantes = get_variantes_bairro()
if variantes is None:
    st.info(
        "Disponível após o reprocessamento com a nova normalização — a coluna "
        "`BAIRRO_ORIGINAL` ainda não existe nos dados publicados."
    )
elif variantes.height == 0:
    st.success("Nenhum bairro com múltiplas variantes de grafia no dataset atual.")
else:
    st.dataframe(
        variantes.to_pandas(),
        use_container_width=True,
        hide_index=True,
        column_config={
            "BAIRRO": st.column_config.TextColumn("Bairro (canônico)"),
            "variantes": st.column_config.NumberColumn("Nº de variantes"),
            "exemplos": st.column_config.TextColumn("Grafias originais"),
            "total_bos": st.column_config.NumberColumn("Total de BOs"),
        },
    )
    st.caption(
        "Encontrou variantes que deveriam ser unificadas e não foram? Elas são "
        "corrigidas via `pipeline/bairro_aliases.csv` no repositório do projeto."
    )

# --- 4. Transformacoes aplicadas ---
st.subheader("⚙️ Transformações aplicadas (raw → transformed)")

st.markdown("""
O pipeline mantém duas camadas no S3 e **nenhum registro é excluído em nenhuma delas**:

**Camada raw** — preserva o Excel da SSP como veio, com apenas:
- Cast de `LATITUDE`/`LONGITUDE` para numérico (troca vírgula decimal por ponto)
- Padronização dos nomes de colunas de ano/mês entre formatos da fonte
- Coluna derivada `TRIMESTRE` a partir de `MES`

**Camada transformed** — lê da raw e adiciona colunas derivadas:

| Coluna | Regra |
|---|---|
| `HAS_COORDINATES` | `LATITUDE` não nula **e** negativa (hemisfério sul) **e** `LONGITUDE` não nula |
| `RUBRICA_MOD` | `RUBRICA` contém "roubo" → Roubo; "furto" → Furto; "perda"/"extravio" → Perda; senão Outros |
| `BAIRRO` | normalizado (ver etapas abaixo) |
| `BAIRRO_ORIGINAL` | valor original de `BAIRRO`, preservado para auditoria |

**Etapas da normalização de bairro**, nesta ordem:
1. Remoção de espaços nas bordas
2. Conversão para maiúsculas
3. Remoção de acentos (`SÃO` → `SAO`)
4. Pontuação vira espaço (`JD.` → `JD`)
5. Colapso de espaços múltiplos
6. Expansão de abreviações comuns (`JD` → `JARDIM`, `VL` → `VILA`, `PQ` → `PARQUE`, `STA` → `SANTA`, …)
7. Aliases manuais mantidos no repositório (`bairro_aliases.csv`)

Valores nulos e vazios passam intactos por todas as etapas — nenhum nome é inventado.
""")
