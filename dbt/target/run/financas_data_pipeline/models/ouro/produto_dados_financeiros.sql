
  
    

  create  table "dw_nerds_prd"."ouro"."produto_dados_financeiros__dbt_tmp"
  
  
    as
  
  (
    

WITH ativos_base AS (
    SELECT
        a.data,
        a.ativo,
        a.preco_abertura,
        a.preco_alto,
        a.preco_baixo,
        a.preco_fechamento,
        a.volume_negociado
    FROM "dw_nerds_prd"."prata"."ativos" a  -- Ref para a tabela `prata.ativos`
),

ativos_com_indicadores AS (
    SELECT
        ab.*,
        i.indicador,
        i.valor_indicador
    FROM ativos_base ab
    LEFT JOIN "dw_nerds_prd"."prata"."indicadores" i  -- Ref para a tabela `prata.indicadores`
    ON ab.data = i.data
    AND ab.ativo = i.ativo
),

ativos_com_tesouro AS (
    SELECT
        ai.*,
        t.rendimento AS rendimento_tesouro
    FROM ativos_com_indicadores ai
    LEFT JOIN "dw_nerds_prd"."prata"."tesouro" t  -- Ref para a tabela `prata.tesouro`
    ON ai.data = t.data
)

SELECT
    data,
    ativo,
    preco_abertura,
    preco_alto,
    preco_baixo,
    preco_fechamento,
    volume_negociado,
    indicador,
    valor_indicador,
    rendimento_tesouro
FROM ativos_com_tesouro
  );
  