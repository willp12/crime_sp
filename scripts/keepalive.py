"""Keepalive do app no Streamlit Community Cloud.

O Community Cloud hiberna apps apos 12h sem trafego. Este script abre o app num
Chromium headless (uma requisicao HTTP simples nao abre sessao no Streamlit e
nao conta como visita) e, se o app estiver dormindo, clica no botao de wake.

A pagina pode estar em tres estados, verificados em polling:

  - dormindo: pagina "Zzzz" servida no frame principal, SEM o iframe do app,
    com o botao [data-testid='wakeup-button-viewer'];
  - acordando: tela de boot do Community Cloud, sem botao nem app;
  - acordado: o app roda num iframe same-origin (src ".../~/+/") e todo o
    conteudo do Streamlit vive DENTRO desse iframe.

Tambem serve de smoke test: se o app renderizar uma excecao (stException), o
script falha e o GitHub Actions avisa por e-mail.

Uso:
    python scripts/keepalive.py [--url URL] [--headed]

Exit code 0 quando o app renderiza sem erro e 1 caso contrario. Na falha,
grava screenshot e HTML em keepalive-artifacts/ para diagnostico.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import FrameLocator, Locator, Page, sync_playwright

URL = "https://crimesp.streamlit.app/"

APP_IFRAME = "iframe[src*='/~/+/']"
WAKE_BUTTON = (
    "[data-testid='wakeup-button-viewer'], "
    "button:has-text('get this app back up'), "
    "button:has-text('wake up')"
)
EXCEPTION = "[data-testid='stException']"
RUNNING = "[data-testid='stStatusWidget'], [data-testid='stSpinner']"

TIMEOUT_S = 600  # o boot apos o sleep reinstala as dependencias e leva minutos
POLL_S = 5
RECLICK_S = 60  # botao de wake ainda visivel depois disso: clica de novo
RUNNING_TIMEOUT_S = 120  # espera o script do app terminar antes do veredito
HOLD_S = 20  # mantem a sessao aberta para a visita ser registrada

ARTIFACTS = Path("keepalive-artifacts")


class AppComErro(Exception):
    """O app renderizou, mas exibindo uma excecao."""


def _visivel(locator: Locator) -> bool:
    """True se o locator casa com algum elemento visivel, sem esperar."""
    return locator.count() > 0 and locator.first.is_visible()


def _abrir(page: Page, url: str) -> None:
    """Abre a URL, com uma nova tentativa em caso de falha de navegacao."""
    for tentativa in (1, 2):
        try:
            print(f"Abrindo {url} (tentativa {tentativa}/2)...")
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            return
        except PlaywrightError as e:
            print(f"Falha ao abrir: {e}")
            if tentativa == 2:
                raise


def _verificar_execucao(page: Page, app: FrameLocator) -> None:
    """Espera o script do app terminar e falha se ele exibir uma excecao.

    Se o script ainda estiver rodando apos RUNNING_TIMEOUT_S, so avisa: a
    visita ja conta como trafego, apenas o veredito do smoke test fica parcial.
    """
    page.wait_for_timeout(3_000)  # o indicador "Running..." surge com atraso
    limite = time.monotonic() + RUNNING_TIMEOUT_S
    while _visivel(app.locator(RUNNING)):
        if time.monotonic() > limite:
            print(f"Aviso: app ainda rodando apos {RUNNING_TIMEOUT_S}s")
            break
        page.wait_for_timeout(2_000)

    if app.locator(EXCEPTION).count() > 0:
        raise AppComErro(app.locator(EXCEPTION).first.inner_text())


def acordar_e_aguardar(page: Page, url: str) -> None:
    """Abre o app, acorda se estiver dormindo e espera ele renderizar.

    Raises:
        AppComErro: o app renderizou uma excecao.
        TimeoutError: o app nao renderizou dentro de TIMEOUT_S.
        PlaywrightError: falha ao abrir a pagina.
    """
    inicio = time.monotonic()
    ultimo_clique: float | None = None
    estado_anterior: str | None = None

    def log_estado(estado: str) -> None:
        nonlocal estado_anterior
        if estado != estado_anterior:
            print(f"[{time.monotonic() - inicio:4.0f}s] {estado}")
            estado_anterior = estado

    def clicar_wake(botao: Locator) -> None:
        nonlocal ultimo_clique
        agora = time.monotonic()
        if ultimo_clique is not None and agora - ultimo_clique < RECLICK_S:
            return
        if ultimo_clique is None:
            print("App dormindo — clicando 'Yes, get this app back up!'...")
        else:
            print("Botao de wake ainda visivel — clicando de novo...")
        botao.first.click(timeout=10_000)
        ultimo_clique = agora

    _abrir(page, url)

    while time.monotonic() - inicio < TIMEOUT_S:
        try:
            if _visivel(page.locator(WAKE_BUTTON)):
                log_estado("Pagina de sleep no frame principal")
                clicar_wake(page.locator(WAKE_BUTTON))
            elif page.locator(APP_IFRAME).count() > 0:
                app = page.frame_locator(APP_IFRAME).first
                if _visivel(app.locator(WAKE_BUTTON)):
                    log_estado("Botao de wake dentro do iframe do app")
                    clicar_wake(app.locator(WAKE_BUTTON))
                elif app.locator(EXCEPTION).count() > 0:
                    raise AppComErro(app.locator(EXCEPTION).first.inner_text())
                elif _visivel(app.locator("h1")):
                    titulo = app.locator("h1").first.inner_text()
                    print(f"App renderizado: {titulo!r}")
                    _verificar_execucao(page, app)
                    page.wait_for_timeout(HOLD_S * 1000)
                    print("Keepalive concluido com sucesso")
                    return
                else:
                    log_estado("Iframe do app carregando")
            else:
                log_estado("Aguardando boot do app")
        except PlaywrightError as e:
            # Durante o boot a pagina recarrega e os locators podem falhar no
            # meio da navegacao: tenta de novo no proximo ciclo.
            log_estado(f"Pagina navegando ({str(e).splitlines()[0]})")
        page.wait_for_timeout(POLL_S * 1000)

    raise TimeoutError(f"App nao renderizou em {TIMEOUT_S}s")


def salvar_evidencias(page: Page) -> None:
    """Grava screenshot e HTML (pagina e iframe do app) para diagnostico."""
    ARTIFACTS.mkdir(exist_ok=True)
    try:
        page.screenshot(path=ARTIFACTS / "screenshot.png", full_page=True)
        (ARTIFACTS / "page.html").write_text(page.content(), encoding="utf-8")
        for frame in page.frames:
            if "/~/+/" in frame.url:
                (ARTIFACTS / "app.html").write_text(frame.content(), encoding="utf-8")
                break
        print(f"Evidencias salvas em {ARTIFACTS}/")
    except PlaywrightError as e:
        print(f"Nao foi possivel salvar evidencias: {e}")


def main() -> int:
    """Executa o keepalive e devolve o exit code."""
    parser = argparse.ArgumentParser(
        description="Mantem o app do Streamlit Community Cloud acordado."
    )
    parser.add_argument("--url", default=URL, help=f"URL do app (default: {URL})")
    parser.add_argument(
        "--headed", action="store_true", help="mostra o navegador (debug local)"
    )
    args = parser.parse_args()

    # O titulo do app tem emoji: nao quebrar em consoles que nao sao UTF-8
    sys.stdout.reconfigure(errors="backslashreplace")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        try:
            acordar_e_aguardar(page, args.url)
            return 0
        except (AppComErro, TimeoutError, PlaywrightError) as e:
            print(f"ERRO ({type(e).__name__}): {e}")
            salvar_evidencias(page)
            return 1
        finally:
            browser.close()


if __name__ == "__main__":
    sys.exit(main())
