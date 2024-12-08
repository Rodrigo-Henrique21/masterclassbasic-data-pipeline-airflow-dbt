WITH tesouro_limpo AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(valor AS NUMERIC(15, 3)) AS rendimento
    FROM {{ ref('bronze.tesouro') }}
    WHERE data IS NOT NULL
      AND valor IS NOT NULL
)

SELECT * FROM tesouro_limpo
