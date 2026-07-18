-- Variantes de grafia unificadas pela normalizacao de bairros
-- Requer BAIRRO_ORIGINAL (existe apos o backfill com o pipeline atualizado)
SELECT
    BAIRRO,
    COUNT(DISTINCT BAIRRO_ORIGINAL) AS variantes,
    STRING_AGG(DISTINCT BAIRRO_ORIGINAL, ' | ') AS exemplos,
    COUNT(*) AS total_bos
FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
WHERE CIDADE = 'S.PAULO'
  AND BAIRRO IS NOT NULL AND BAIRRO != ''
GROUP BY BAIRRO
HAVING COUNT(DISTINCT BAIRRO_ORIGINAL) > 1
ORDER BY variantes DESC, total_bos DESC
LIMIT 100
