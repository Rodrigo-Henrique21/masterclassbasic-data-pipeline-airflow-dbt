
  
    

  create  table "dw_nerds_prd"."prata"."tesouro__dbt_tmp"
  
  
    as
  
  (
    

WITH tesouro_limpo AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(valor AS NUMERIC(15, 3)) AS rendimento
    FROM "dw_nerds_prd"."bronze"."tesouro"
    WHERE data IS NOT NULL AND valor IS NOT NULL
)

SELECT
    data,
    rendimento
FROM tesouro_limpo
  );
  