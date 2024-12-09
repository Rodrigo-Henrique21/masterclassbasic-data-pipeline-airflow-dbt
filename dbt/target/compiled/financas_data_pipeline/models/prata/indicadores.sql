

WITH indicadores_limpos AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(valor AS NUMERIC(15, 3)) AS valor_indicador,
        ticker AS ativo, -- Renomeando ticker para ativo
        indicador
    FROM "dw_nerds_prd"."bronze"."indicadores"
    WHERE data IS NOT NULL
      AND valor IS NOT NULL
)

SELECT
    data,
    valor_indicador,
    ativo, -- Mantendo o nome correto como ativo
    indicador
FROM indicadores_limpos