"""Camada de tiles (mapa base) do mapa principal."""

from __future__ import annotations

from urllib.parse import quote

import folium

# Desde os termos de 29/09/2026 o CARTO exige ?key= em toda URL de
# basemaps.cartocdn.com: sem key, cada tile vem com a marca d'agua
# "API KEY REQUIRED". Key gratuita para uso nao comercial, pedida por e-mail
# em https://carto.com/basemaps/apikey (secao [carto] dos secrets).
CARTO_POSITRON_URL = (
    "https://basemaps.cartocdn.com/rastertiles/light_all/{z}/{x}/{y}.png"
)

# Esse endpoint so gera tiles ate o zoom 18: no 19 e no 20 a requisicao falha
# e o mapa fica cinza, sem ruas. Acima do zoom nativo o Leaflet amplia os
# tiles do 18, e os nomes de rua continuam visiveis (um pouco menos nitidos).
CARTO_MAX_NATIVE_ZOOM = 18

# Atribuicao exigida pelos termos do CARTO em todo mapa, inclusive no plano free
CARTO_ATTR = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
    'contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
)


def criar_tile_layer(api_key: str | None) -> folium.TileLayer:
    """Retorna a camada de tiles do mapa base.

    Com key, usa o CARTO Positron (fundo claro que destaca os clusters). Sem
    key, cai no OpenStreetMap: o mapa fica mais carregado, mas nunca exibe a
    marca d'agua do CARTO.

    Args:
        api_key: Key do CARTO Basemaps ou None.

    Returns:
        TileLayer pronta para ``folium.Map(tiles=...)``.
    """
    key = (api_key or "").strip()
    if not key:
        return folium.TileLayer("OpenStreetMap")

    return folium.TileLayer(
        tiles=f"{CARTO_POSITRON_URL}?key={quote(key, safe='')}",
        attr=CARTO_ATTR,
        name="CARTO Positron",
        max_zoom=20,
        max_native_zoom=CARTO_MAX_NATIVE_ZOOM,
    )
