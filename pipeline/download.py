"""Download de Excel de celulares subtraidos do Portal SSP-SP.

URL direta: https://www.ssp.sp.gov.br/assets/estatistica/transparencia/baseDados/celularesSub/CelularesSubtraidos_{ANO}.xlsx
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

BASE_URL = (
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/"
    "baseDados/celularesSub/CelularesSubtraidos_{year}.xlsx"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
}


def download_excel(year: int, output_dir: Path) -> Path:
    """Baixa Excel de celulares subtraidos para um ano.

    Args:
        year: Ano dos dados.
        output_dir: Diretorio de saida.

    Returns:
        Path do arquivo baixado.
    """
    url = BASE_URL.format(year=year)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"CelularesSubtraidos_{year}.xlsx"

    print(f"Baixando: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=120)

    if resp.status_code == 404:
        print(f"ERRO: Dados nao encontrados para {year} (404)")
        sys.exit(1)

    resp.raise_for_status()

    # Verificar se e um arquivo Excel valido (comeca com PK = ZIP)
    if not resp.content[:2] == b"PK":
        print("ERRO: Resposta nao e um arquivo Excel valido.")
        print(f"Content-Type: {resp.headers.get('content-type')}")
        print(f"Primeiros bytes: {resp.content[:50]}")
        sys.exit(1)

    out_path.write_bytes(resp.content)
    size_mb = len(resp.content) / (1024 * 1024)
    print(f"Salvo: {out_path} ({size_mb:.1f} MB)")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Excel do Portal SSP-SP")
    parser.add_argument(
        "--ano",
        type=int,
        required=True,
        help="Ano para download (ex: 2024)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/raw",
        help="Diretorio de saida (default: /tmp/raw)",
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path de arquivo Excel local (pula download do portal)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output)

    # Fallback: arquivo local
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"ERRO: Arquivo nao encontrado: {input_path}")
            sys.exit(1)
        output_dir.mkdir(parents=True, exist_ok=True)
        dest = output_dir / input_path.name
        if dest != input_path:
            dest.write_bytes(input_path.read_bytes())
            print(f"Copiado: {input_path} -> {dest}")
        else:
            print(f"Arquivo ja esta no diretorio de saida: {dest}")
        return

    download_excel(args.ano, output_dir)


if __name__ == "__main__":
    main()
