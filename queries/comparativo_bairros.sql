-- Comparacao por bairro entre dois anos (mesmos trimestres)
-- Delta absoluto e percentual sao calculados em Polars no app
-- Sem union_by_name: todas as colunas referenciadas existem em todos os anos,
-- e union_by_name + filtro de ANO/TRIMESTRE dispara erro interno de pushdown
-- no DuckDB (SetMin/SetMax de estatisticas de Parquet)
SELECT
    BAIRRO,
    COUNT(*) FILTER (WHERE ANO = {ano_a}) AS total_atual,
    COUNT(*) FILTER (WHERE ANO = {ano_b}) AS total_anterior
FROM read_parquet('{s3_path}', hive_partitioning=true)
WHERE CIDADE = 'S.PAULO'
  AND ANO IN ({ano_a}, {ano_b})
  AND TRIMESTRE IN ({trimestres})
  AND BAIRRO IS NOT NULL AND BAIRRO != ''
GROUP BY BAIRRO
ORDER BY total_atual DESC
