
  
    

  create  table "dw_nerds_prd"."prata"."ativos__dbt_tmp"
  
  
    as
  
  (
    

WITH ativos_limpos AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(open AS NUMERIC(15, 3)) AS preco_abertura,
        CAST(high AS NUMERIC(15, 3)) AS preco_alto,
        CAST(low AS NUMERIC(15, 3)) AS preco_baixo,
        CAST(close AS NUMERIC(15, 3)) AS preco_fechamento,
        CAST(volume AS BIGINT) AS volume_negociado,
        ticker AS ativo -- Renomeando ticker para ativo
    FROM "dw_nerds_prd"."bronze"."ativos"
    WHERE data IS NOT NULL
)

SELECT
    data,
    preco_abertura,
    preco_alto,
    preco_baixo,
    preco_fechamento,
    volume_negociado,
    ativo -- Mantendo o nome correto como ativo
FROM ativos_limpos
  );
  