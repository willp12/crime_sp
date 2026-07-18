-- Metricas de data quality por particao (pagina de qualidade)
-- union_by_name: colunas ausentes em anos antigos viram NULL em vez de erro
SELECT
    ANO AS ANO,
    TRIMESTRE AS TRIMESTRE,
    COUNT(*) AS linhas,
    COUNT(DISTINCT COALESCE(CAST(ANO_BO AS VARCHAR), '') || '/' || COALESCE(NUM_BO, '')) AS bos_unicos,
    CAST(SUM(CASE WHEN TRY_CAST(VERSAO AS INTEGER) > 1 THEN 1 ELSE 0 END) AS BIGINT) AS linhas_versao_maior_1,
    CAST(SUM(CASE WHEN HAS_COORDINATES THEN 1 ELSE 0 END) AS BIGINT) AS com_coordenadas,
    CAST(SUM(CASE WHEN BAIRRO IS NULL OR BAIRRO = '' THEN 1 ELSE 0 END) AS BIGINT) AS bairro_vazio,
    CAST(SUM(CASE WHEN DESCR_PERIODO IS NULL OR DESCR_PERIODO = 'NULL' THEN 1 ELSE 0 END) AS BIGINT) AS periodo_nulo,
    CAST(SUM(CASE WHEN MARCA_OBJETO IS NULL OR MARCA_OBJETO = '' OR MARCA_OBJETO = 'NULL' THEN 1 ELSE 0 END) AS BIGINT) AS marca_vazia
FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
WHERE CIDADE = 'S.PAULO'
GROUP BY ANO, TRIMESTRE
ORDER BY ANO, TRIMESTRE
