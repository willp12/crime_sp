"""Transformacao de Excel para Parquet em duas camadas: raw e transformed.

Raw: dado bruto do Excel com cast de tipos + coluna TRIMESTRE.
Transformed: dados enriquecidos com RUBRICA_MOD, HAS_COORDINATES, normalizacao de BAIRRO.
Nenhum registro e excluido em nenhuma das camadas.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl


def read_excel(excel_path: Path) -> pl.DataFrame:
    """Le Excel com Polars (usa fastexcel como backend).

    Detecta automaticamente a aba com os dados (CELULAR_YYYY ou primeira aba grande).
    """
    import fastexcel

    print(f"Lendo Excel: {excel_path}")
    wb = fastexcel.read_excel(str(excel_path))
    sheets = wb.sheet_names

    # Procurar aba que comeca com CELULAR_
    target_sheet = None
    for name in sheets:
        if name.upper().startswith("CELULAR_"):
            target_sheet = name
            break

    if target_sheet:
        print(f"  Usando aba: {target_sheet}")
        df = pl.read_excel(excel_path, sheet_name=target_sheet)
    else:
        # Fallback: primeira aba (formato antigo como 2024)
        df = pl.read_excel(excel_path)

    print(f"  {df.shape[0]} linhas x {df.shape[1]} colunas")
    return df


def _quarter_from_month(month: int) -> int:
    """Retorna trimestre a partir do mes."""
    return (month - 1) // 3 + 1


def _write_partitioned(df: pl.DataFrame, output_dir: Path, layer_name: str) -> None:
    """Salva DataFrame particionado por ANO/TRIMESTRE no formato hive."""
    for (ano, trimestre), partition_df in df.partition_by(
        ["ANO", "TRIMESTRE"], as_dict=True, include_key=True
    ).items():
        part_dir = output_dir / f"ano={ano}" / f"trimestre={trimestre}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / "data.parquet"
        partition_df.write_parquet(out_path)
        print(f"  {layer_name}: {out_path} ({partition_df.shape[0]} registros)")


def save_raw(df: pl.DataFrame, output_dir: Path) -> None:
    """Salva camada raw: dado bruto com cast de tipos + TRIMESTRE.

    Nenhum registro e excluido.
    """
    # Cast de coordenadas para Float64 (caso venham como string)
    for coord_col in ("LATITUDE", "LONGITUDE"):
        if coord_col in df.columns and df[coord_col].dtype in (pl.String, pl.Utf8):
            df = df.with_columns(
                pl.col(coord_col).str.replace(",", ".").cast(pl.Float64, strict=False)
            )

    # Normalizar nomes de colunas ANO/MES (2025+ usa ANO_REGISTRO_BO/MES_REGISTRO_BO)
    if "ANO" not in df.columns and "ANO_REGISTRO_BO" in df.columns:
        df = df.rename({"ANO_REGISTRO_BO": "ANO"})
    if "MES" not in df.columns and "MES_REGISTRO_BO" in df.columns:
        df = df.rename({"MES_REGISTRO_BO": "MES"})

    # Adicionar TRIMESTRE a partir de MES
    if "MES" in df.columns and "TRIMESTRE" not in df.columns:
        df = df.with_columns(
            ((pl.col("MES") - 1) // 3 + 1).cast(pl.Int32).alias("TRIMESTRE")
        )

    # Garantir ANO como Int32
    if "ANO" in df.columns:
        df = df.with_columns(pl.col("ANO").cast(pl.Int32))

    _write_partitioned(df, output_dir, "Raw")


def save_transformed(raw_dir: Path, output_dir: Path) -> None:
    """Le da camada raw e gera camada transformed com colunas derivadas.

    Nenhum registro e excluido. Registros sem coordenadas permanecem.
    """
    # Ler todos os Parquets da camada raw com hive partitioning
    parquet_files = sorted(raw_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"ERRO: Nenhum arquivo Parquet encontrado em {raw_dir}")
        sys.exit(1)

    all_dfs = []
    for pq_file in parquet_files:
        part_df = pl.read_parquet(pq_file)
        # Extrair ANO e TRIMESTRE do path hive
        ano, trimestre = _extract_partition_from_path(pq_file)
        if ano is not None and trimestre is not None:
            part_df = part_df.with_columns(
                pl.lit(ano).cast(pl.Int32).alias("ANO"),
                pl.lit(trimestre).cast(pl.Int32).alias("TRIMESTRE"),
            )
        all_dfs.append(part_df)

    df = pl.concat(all_dfs, how="diagonal")
    print(f"  Total raw: {df.shape[0]} registros")

    # HAS_COORDINATES: latitude negativa (hemisferio sul) e longitude nao nula
    if "LATITUDE" in df.columns and "LONGITUDE" in df.columns:
        df = df.with_columns(
            (
                pl.col("LATITUDE").is_not_null()
                & (pl.col("LATITUDE") < 0)
                & pl.col("LONGITUDE").is_not_null()
            ).alias("HAS_COORDINATES")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("HAS_COORDINATES"))

    # RUBRICA_MOD: categorizar tipo de crime
    if "RUBRICA" in df.columns:
        rubrica_lower = pl.col("RUBRICA").str.to_lowercase()
        df = df.with_columns(
            pl.when(rubrica_lower.str.contains("roubo"))
            .then(pl.lit("Roubo"))
            .when(rubrica_lower.str.contains("furto"))
            .then(pl.lit("Furto"))
            .when(rubrica_lower.str.contains("perda") | rubrica_lower.str.contains("extravio"))
            .then(pl.lit("Perda"))
            .otherwise(pl.lit("Outros"))
            .alias("RUBRICA_MOD")
        )

    # Normalizar BAIRRO (upper, trim)
    if "BAIRRO" in df.columns:
        df = df.with_columns(
            pl.col("BAIRRO").str.strip_chars().str.to_uppercase()
        )

    # Estatisticas
    total = df.shape[0]
    with_coords = df.filter(pl.col("HAS_COORDINATES")).shape[0]
    print(f"  {with_coords}/{total} registros com coordenadas ({with_coords/total*100:.1f}%)")

    if "RUBRICA_MOD" in df.columns:
        print("  Distribuicao RUBRICA_MOD:")
        for row in df["RUBRICA_MOD"].value_counts().sort("count", descending=True).iter_rows():
            print(f"    {row[0]}: {row[1]}")

    _write_partitioned(df, output_dir, "Transformed")


def _extract_partition_from_path(path: Path) -> tuple[int | None, int | None]:
    """Extrai ano e trimestre do path hive-partitioned."""
    ano = None
    trimestre = None
    for part in path.parts:
        if part.startswith("ano="):
            try:
                ano = int(part.split("=")[1])
            except ValueError:
                pass
        elif part.startswith("trimestre="):
            try:
                trimestre = int(part.split("=")[1])
            except ValueError:
                pass
    return ano, trimestre


def main() -> None:
    parser = argparse.ArgumentParser(description="Transforma Excel em Parquet (raw + transformed)")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Diretorio com arquivos Excel (para raw) ou camada raw (para transformed)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Diretorio base de saida para os Parquets",
    )
    parser.add_argument(
        "--layer",
        type=str,
        choices=["raw", "transformed", "all"],
        default="all",
        help="Camada a processar (default: all)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if args.layer in ("raw", "all"):
        print("=== Processando camada RAW ===")
        excel_files = sorted(input_path.glob("*.xlsx")) + sorted(input_path.glob("*.xls"))
        if not excel_files:
            print(f"ERRO: Nenhum arquivo Excel encontrado em {input_path}")
            sys.exit(1)

        raw_output = output_path / "raw" / "parquet"
        for excel_file in excel_files:
            print(f"\nProcessando: {excel_file.name}")
            df = read_excel(excel_file)
            save_raw(df, raw_output)

    if args.layer in ("transformed", "all"):
        print("\n=== Processando camada TRANSFORMED ===")
        raw_input = output_path / "raw" / "parquet" if args.layer == "all" else input_path
        transformed_output = output_path / "transformed" / "parquet"
        save_transformed(raw_input, transformed_output)

    print("\nTransformacao concluida!")


if __name__ == "__main__":
    main()
