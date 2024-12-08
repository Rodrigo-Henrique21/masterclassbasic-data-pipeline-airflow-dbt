WITH indicadores_limpos AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(valor AS NUMERIC(15, 3)) AS valor_indicador,
        ticker AS ativo,
        indicador
    FROM {{ ref('bronze.indicadores') }}
    WHERE data IS NOT NULL
      AND valor IS NOT NULL
)

SELECT 
    data,
    ativo,
    indicador,
    valor_indicador
FROM indicadores_limpos
