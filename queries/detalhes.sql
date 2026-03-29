-- Query para tabela detalhada
SELECT
    NUM_BO,
    DATA_OCORRENCIA_BO,
    DESCR_PERIODO,
    RUBRICA_MOD,
    BAIRRO,
    LOGRADOURO,
    MARCA_OBJETO
FROM read_parquet('{s3_path}', hive_partitioning=true)
WHERE CIDADE = 'S.PAULO'
  AND ANO = {ano}
  AND TRIMESTRE IN ({trimestres})
  {where_extra}
ORDER BY DATA_OCORRENCIA_BO DESC
LIMIT 1000
