"""Componentes de UI compartilhados entre as paginas do app."""

from __future__ import annotations

from datetime import date, datetime

import streamlit as st

from src.data import get_manifest, get_ultimo_registro


def _fmt(d: date) -> str:
    """Formata data como DD/MM/YYYY."""
    return d.strftime("%d/%m/%Y")


def render_freshness_caption() -> None:
    """Exibe caption com a data da ultima carga e do registro mais recente.

    Nao renderiza nada se nenhuma das informacoes estiver disponivel — a
    pagina nunca quebra (o manifest so existe apos o primeiro run do
    pipeline atualizado).
    """
    atualizado: str | None = None
    manifest = get_manifest()
    if manifest and manifest.get("processed_at"):
        try:
            processado = datetime.fromisoformat(manifest["processed_at"])
            atualizado = _fmt(processado.date())
        except ValueError:
            pass

    ultimo = get_ultimo_registro()

    if atualizado and ultimo:
        st.caption(
            f"🕒 Dados atualizados em **{atualizado}** · "
            f"registro mais recente: **{_fmt(ultimo)}**"
        )
    elif atualizado:
        st.caption(f"🕒 Dados atualizados em **{atualizado}**")
    elif ultimo:
        st.caption(f"🕒 Registro mais recente: **{_fmt(ultimo)}**")
