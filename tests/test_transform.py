"""Testes das derivacoes das camadas raw e transformed (dados sinteticos, sem rede)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.transform import save_raw, save_transformed


@pytest.fixture()
def df_sintetico() -> pl.DataFrame:
    """Amostra sintetica cobrindo casos de coordenadas, rubrica e bairro."""
    return pl.DataFrame(
        {
            "NUM_BO": ["A1", "A2", "A3", "A4", "A5", "A6"],
            "ANO": [2026] * 6,
            "MES": [1, 2, 4, 7, 11, 12],
            "CIDADE": ["S.PAULO"] * 6,
            "RUBRICA": [
                "Roubo (art. 157)",
                "Furto (art. 155)",
                "Perda de objeto",
                "Extravio de documento",
                "Apropriacao indebita",
                "Estelionato",
            ],
            "BAIRRO": [
                "  jd  paulista ",
                "VL. MARIANA",
                "SÃO JOÃO CLÍMACO",
                None,
                "",
                "CENTRO",
            ],
            "LATITUDE": ["-23,55", None, "-23.60", "10.0", "-23,70", None],
            "LONGITUDE": ["-46,63", "-46.60", None, "-46.00", "-46,70", None],
        }
    )


def _ler_camada(base_dir: Path) -> pl.DataFrame:
    """Le todos os Parquets de uma camada e concatena."""
    files = sorted(base_dir.rglob("*.parquet"))
    assert files, f"nenhum parquet gerado em {base_dir}"
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal")


def test_save_raw_deriva_trimestre_e_casts(df_sintetico: pl.DataFrame, tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "parquet"
    save_raw(df_sintetico, raw_dir)

    out = _ler_camada(raw_dir).sort("NUM_BO")

    # Nenhum registro excluido
    assert out.height == df_sintetico.height

    # TRIMESTRE derivado de MES: 1,2 -> Q1; 4 -> Q2; 7 -> Q3; 11,12 -> Q4
    assert out["TRIMESTRE"].to_list() == [1, 1, 2, 3, 4, 4]

    # Coordenadas viram Float64, com virgula decimal tratada
    assert out["LATITUDE"].dtype == pl.Float64
    assert out["LATITUDE"].to_list() == [-23.55, None, -23.6, 10.0, -23.7, None]

    # Particionamento hive por ano/trimestre
    assert (raw_dir / "ano=2026" / "trimestre=1" / "data.parquet").exists()
    assert (raw_dir / "ano=2026" / "trimestre=4" / "data.parquet").exists()


def test_save_raw_renomeia_colunas_formato_2025(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {"NUM_BO": ["B1"], "ANO_REGISTRO_BO": [2025], "MES_REGISTRO_BO": [8]}
    )
    raw_dir = tmp_path / "raw" / "parquet"
    save_raw(df, raw_dir)

    files = list(raw_dir.rglob("*.parquet"))
    assert len(files) == 1
    assert "ano=2025" in files[0].as_posix()
    assert "trimestre=3" in files[0].as_posix()

    out = pl.read_parquet(files[0])
    assert out["ANO"][0] == 2025
    assert out["TRIMESTRE"][0] == 3


def test_save_transformed_derivacoes(df_sintetico: pl.DataFrame, tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "parquet"
    transformed_dir = tmp_path / "transformed" / "parquet"
    save_raw(df_sintetico, raw_dir)
    save_transformed(raw_dir, transformed_dir)

    out = _ler_camada(transformed_dir).sort("NUM_BO")

    # Nenhum registro excluido (BOs sem coordenadas permanecem)
    assert out.height == df_sintetico.height

    # HAS_COORDINATES: lat nao nula E negativa E lon nao nula
    assert out["HAS_COORDINATES"].to_list() == [True, False, False, False, True, False]

    # RUBRICA_MOD por substring da RUBRICA
    assert out["RUBRICA_MOD"].to_list() == [
        "Roubo",
        "Furto",
        "Perda",
        "Perda",
        "Outros",
        "Outros",
    ]

    # BAIRRO normalizado; original preservado em BAIRRO_ORIGINAL
    assert out["BAIRRO"].to_list() == [
        "JARDIM PAULISTA",
        "VILA MARIANA",
        "SAO JOAO CLIMACO",
        None,
        "",
        "CENTRO",
    ]
    assert out["BAIRRO_ORIGINAL"].to_list() == [
        "  jd  paulista ",
        "VL. MARIANA",
        "SÃO JOÃO CLÍMACO",
        None,
        "",
        "CENTRO",
    ]
