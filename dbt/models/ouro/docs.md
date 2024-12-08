{% docs ativos_ouro_docs %}
Tabela consolidada da camada Ouro contendo informações financeiras dos ativos, indicadores técnicos e rendimento do Tesouro. Esta tabela unifica os dados das camadas Prata para fornecer uma visão completa e pronta para análises.

### Fontes
- **ativos_prata**: Informações básicas de preços e volumes dos ativos.
- **indicadores_prata**: Indicadores técnicos calculados para os ativos.
- **tesouro_prata**: Rendimento do Tesouro associado às datas dos registros.

### Colunas
- **data**: Data do registro consolidado.
- **ativo**: Código do ativo (ticker) no mercado financeiro.
- **preco_abertura**: Preço de abertura do ativo no dia.
- **preco_alto**: Preço mais alto do ativo no dia.
- **preco_baixo**: Preço mais baixo do ativo no dia.
- **preco_fechamento**: Preço de fechamento do ativo no dia.
- **volume_negociado**: Volume total de negociações do ativo no dia.
- **indicador**: Nome do indicador técnico associado ao ativo.
- **valor_indicador**: Valor calculado do indicador técnico.
- **rendimento_tesouro**: Rendimento do Tesouro americano na data.

### Uso
Esta tabela é usada para:
- **Análises financeiras completas**: Combina dados de preços, indicadores técnicos e rendimento do Tesouro.
- **Relatórios executivos**: Oferece uma visão consolidada e limpa dos dados financeiros.
- **Estudos de correlação**: Permite comparar movimentos de ativos com indicadores técnicos e rendimento do Tesouro.
{% enddocs %}
v