{% docs ativos_docs %}
Tabela refinada de ativos financeiros, ajustada para padronizar os dados, traduzir colunas e remover inconsistências.

### Colunas
- **data**: Data do registro.
- **preco_abertura**: Preço de abertura do ativo no dia.
- **preco_alto**: Preço mais alto do ativo no dia.
- **preco_baixo**: Preço mais baixo do ativo no dia.
- **preco_fechamento**: Preço de fechamento do ativo no dia.
- **volume_negociado**: Volume total de negociações do ativo no dia.
- **ativo**: Código do ativo.
{% enddocs %}

{% docs indicadores_docs %}
Tabela refinada de indicadores técnicos, ajustada para padronizar os dados e traduzir colunas.

### Colunas
- **data**: Data do registro.
- **ativo**: Código do ativo.
- **indicador**: Nome do indicador técnico.
- **valor_indicador**: Valor do indicador técnico para o ativo no dia.
{% enddocs %}

{% docs tesouro_docs %}
Tabela refinada do Tesouro, ajustada para padronizar os dados, traduzir colunas e remover inconsistências.

### Colunas
- **data**: Data do registro.
- **rendimento**: Rendimento do Tesouro no dia.
{% enddocs %}
