-- Serie mensal agregada de dois anos para o comparativo anual
-- Saida pequena (<= 2 anos x 12 meses x categorias); totais derivados em Polars
-- Sem union_by_name: todas as colunas referenciadas existem em todos os anos,
-- e union_by_name + filtro de ANO/TRIMESTRE dispara erro interno de pushdown
-- no DuckDB (SetMin/SetMax de estatisticas de Parquet)
SELECT
    ANO AS ANO,
    MES AS MES,
    RUBRICA_MOD,
    COUNT(*) AS total,
    CAST(SUM(CASE WHEN HAS_COORDINATES THEN 1 ELSE 0 END) AS BIGINT) AS com_coordenadas
FROM read_parquet('{s3_path}', hive_partitioning=true)
WHERE CIDADE = 'S.PAULO'
  AND ANO IN ({ano_a}, {ano_b})
  AND TRIMESTRE IN ({trimestres})
GROUP BY ANO, MES, RUBRICA_MOD
ORDER BY ANO, MES
