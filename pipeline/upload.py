"""Upload de Parquets (raw + transformed) para AWS S3.

Estrutura no bucket:
  s3://crime-data-sp/raw/parquet/ano=YYYY/trimestre=Q/data.parquet
  s3://crime-data-sp/transformed/parquet/ano=YYYY/trimestre=Q/data.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def get_s3_client() -> boto3.client:
    """Cria cliente S3. Usa env vars (CI) ou credenciais do aws configure (local)."""
    kwargs: dict = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    # Se env vars existem, usa explicitamente (GitHub Actions)
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        kwargs["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    return boto3.client("s3", **kwargs)


def upload_directory(
    s3_client: boto3.client,
    local_dir: Path,
    bucket: str,
    s3_prefix: str,
) -> int:
    """Faz upload de todos os Parquets de um diretorio para S3.

    Args:
        s3_client: Cliente boto3 S3.
        local_dir: Diretorio local com os Parquets.
        bucket: Nome do bucket S3.
        s3_prefix: Prefixo no S3 (ex: 'raw/parquet' ou 'transformed/parquet').

    Returns:
        Numero de arquivos enviados.
    """
    if not local_dir.exists():
        print(f"ERRO: Diretorio nao encontrado: {local_dir}")
        return 0

    parquet_files = list(local_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"AVISO: Nenhum arquivo Parquet em {local_dir}")
        return 0

    uploaded = 0
    for pq_file in parquet_files:
        # Construir key S3 mantendo estrutura hive
        relative = pq_file.relative_to(local_dir)
        s3_key = f"{s3_prefix}/{relative.as_posix()}"

        print(f"  Uploading: {pq_file} -> s3://{bucket}/{s3_key}")
        try:
            s3_client.upload_file(str(pq_file), bucket, s3_key)
            uploaded += 1
        except ClientError as e:
            print(f"  ERRO no upload: {e}")
            sys.exit(1)

    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload Parquets para S3")
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Diretorio base com as camadas raw/ e transformed/",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("AWS_S3_BUCKET", "crime-data-sp"),
        help="Nome do bucket S3 (default: crime-data-sp ou AWS_S3_BUCKET env var)",
    )
    parser.add_argument(
        "--layer",
        type=str,
        choices=["raw", "transformed", "all"],
        default="all",
        help="Camada a enviar (default: all)",
    )
    args = parser.parse_args()

    source = Path(args.source)
    bucket = args.bucket
    s3_client = get_s3_client()

    # Verificar acesso ao bucket
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' acessivel.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"ERRO: Bucket '{bucket}' nao encontrado.")
        elif error_code == "403":
            print(f"ERRO: Sem permissao para acessar bucket '{bucket}'.")
        else:
            print(f"ERRO ao acessar bucket: {e}")
        sys.exit(1)

    total_uploaded = 0

    if args.layer in ("raw", "all"):
        raw_dir = source / "raw" / "parquet"
        print(f"\n=== Upload camada RAW ===")
        count = upload_directory(s3_client, raw_dir, bucket, "raw/parquet")
        total_uploaded += count
        print(f"  {count} arquivo(s) enviado(s)")

    if args.layer in ("transformed", "all"):
        transformed_dir = source / "transformed" / "parquet"
        print(f"\n=== Upload camada TRANSFORMED ===")
        count = upload_directory(s3_client, transformed_dir, bucket, "transformed/parquet")
        total_uploaded += count
        print(f"  {count} arquivo(s) enviado(s)")

    print(f"\nUpload concluido: {total_uploaded} arquivo(s) total")


if __name__ == "__main__":
    main()
