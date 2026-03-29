"""Pagina Sobre — metodologia, fonte dos dados e stack tecnica."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Crime SP — Sobre", page_icon="🗺️", layout="wide")

st.title("ℹ️ Sobre o Projeto")

st.markdown("""
## Fonte dos dados

Os dados são obtidos do **Portal da Secretaria de Segurança Pública do Estado de São Paulo
(SSP-SP)**, disponíveis para download público em formato Excel (.xlsx).

- **Página de consultas:** [SSP-SP — Estatística / Consultas](https://www.ssp.sp.gov.br/estatistica/consultas)
- **Tipo de dado:** Celulares subtraídos (roubos, furtos, perdas e outros)
- **Cobertura temporal:** 2023 em diante
- **Frequência de atualização:** Trimestral
- **Abrangência geográfica:** Estado de São Paulo (filtrado para a capital no app)

---

## Metodologia

### Pipeline de dados

1. **Download** do Excel anual do Portal SSP-SP
2. **Camada raw:** preserva o dado bruto com cast de tipos e particionamento por ano/trimestre
3. **Camada transformed:** enriquece com colunas derivadas:
   - `RUBRICA_MOD` — categorização simplificada (Roubo, Furto, Perda, Outros)
   - `HAS_COORDINATES` — flag de geolocalização válida
   - Normalização de nomes de bairros
4. **Upload** para AWS S3 em formato Parquet particionado

### Regras importantes

- **Nenhum registro é excluído** em nenhuma das camadas
- O filtro de coordenadas (`HAS_COORDINATES = true`) é aplicado **apenas na query do mapa**
- As estatísticas usam **todos os BOs**, independente de terem coordenadas

---

## Stack técnica

| Componente | Tecnologia |
|---|---|
| Linguagem | Python 3.12+ |
| Processamento | Polars |
| Storage | AWS S3 (Parquet) |
| Query engine | DuckDB + httpfs |
| App | Streamlit |
| Mapas | Folium + FastMarkerCluster |
| Gráficos | Plotly Express |
| CI/CD | GitHub Actions |
| Deploy | Streamlit Community Cloud |

---

## Repositório

Código-fonte disponível no GitHub:
[github.com/seu-usuario/crime-data-sp](https://github.com/seu-usuario/crime-data-sp)

---

## Autor

Projeto de portfólio em Data Engineering & Analytics.
""")
