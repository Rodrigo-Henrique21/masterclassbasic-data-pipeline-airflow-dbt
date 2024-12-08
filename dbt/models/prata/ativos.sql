WITH ativos_limpos AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(open AS NUMERIC(15, 3)) AS preco_abertura,
        CAST(high AS NUMERIC(15, 3)) AS preco_alto,
        CAST(low AS NUMERIC(15, 3)) AS preco_baixo,
        CAST(close AS NUMERIC(15, 3)) AS preco_fechamento,
        CAST(volume AS BIGINT) AS volume_negociado,
        ticker AS ativo
    FROM {{ ref('bronze.ativos') }}
    WHERE data IS NOT NULL
)

SELECT * FROM ativos_limpos
