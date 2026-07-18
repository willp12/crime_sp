-- Contagens por particao: tabela de particoes da pagina de qualidade e
-- descoberta de trimestres comparaveis no comparativo anual
-- Aliases explicitos garantem nomes em maiusculas mesmo quando a coluna vem
-- do path hive; CAST evita dtype decimal no Polars
SELECT
    ANO AS ANO,
    TRIMESTRE AS TRIMESTRE,
    COUNT(*) AS total,
    CAST(SUM(CASE WHEN HAS_COORDINATES THEN 1 ELSE 0 END) AS BIGINT) AS com_coordenadas
FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
WHERE CIDADE = 'S.PAULO'
GROUP BY ANO, TRIMESTRE
ORDER BY ANO, TRIMESTRE
