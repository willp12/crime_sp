-- Query para dados do mapa: apenas BOs com coordenadas validas
-- Filtro HAS_COORDINATES = true acontece APENAS aqui
SELECT
    LATITUDE,
    LONGITUDE,
    RUBRICA_MOD,
    BAIRRO,
    DESCR_PERIODO,
    LOGRADOURO,
    NUM_BO,
    DATA_OCORRENCIA_BO,
    MARCA_OBJETO
FROM read_parquet('{s3_path}', hive_partitioning=true)
WHERE CIDADE = 'S.PAULO'
  AND HAS_COORDINATES = true
  AND ANO = {ano}
  AND TRIMESTRE IN ({trimestres})
  {where_extra}
