-- Query para estatisticas: usa TODOS os BOs (sem filtro de coordenadas)
SELECT
    NUM_BO,
    DATA_OCORRENCIA_BO,
    DESCR_PERIODO,
    RUBRICA,
    RUBRICA_MOD,
    BAIRRO,
    LOGRADOURO,
    MARCA_OBJETO,
    HAS_COORDINATES,
    MES,
    ANO,
    TRIMESTRE
FROM read_parquet('{s3_path}', hive_partitioning=true)
WHERE CIDADE = 'S.PAULO'
  AND ANO = {ano}
  AND TRIMESTRE IN ({trimestres})
  {where_extra}
