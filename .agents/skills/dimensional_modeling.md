---
name: Boilerplate de Modelagem Dimensional
description: Padrões e boas práticas para criar a modelagem dimensional no dbt.
---

# Skill: Modelagem Dimensional

Para executar o desafio focado na camada semântica com dbt, deve-se aplicar as seguintes diretrizes na construção do Data Warehouse.

## Conceitos Fundamentais
1. **Star Schema Centralizado**:
   - Defina os fatos com nível de **granularidade (grain)** o mais detalhado possível.
   - Utilize dimensões conectadas em formato estrela (modelo desnormalizado para consultas de alta performance nas extremidades analíticas).
   
2. **Slowly Changing Dimensions (SCDs)**:
   - Para entidades que mudam ao longo do tempo (por ex. `spaces`, que mudam de `tier` ou `capacity`), deve-se aplicar o **SCD Type 2**.
   - SCD Type 2 mantém o histórico de cada modificação gerando colunas `valid_from`, `valid_to` e `is_current_version` ou gerado via snapshots nativos do dbt.
   - Outras tabelas podem usar SCD Type 1 se o tracking for tido por irrelevante ou sobrecarregar a volumetria sem necessidade de reperspectiva.

3. **Lidando com Dados Ausentes**:
   - Para completar um modelo, se campos que compõem dimensões ricas parecem ausentes nos sources `raw_`, eles devem ser anotados para levantamento futuro.
   - A documentação de quais tabelas precisam ser requisitadas aos Stakeholders é mandatória, sempre propondo uma governança forte (e.g. tracking via Segment/Snowplow, CRM).

4. **Agentes de IA e Modelos Dimensional**:
   - Modelos clássicos são montados para Business Intelligence, focando fortemente em métricas pré-agregadas.
   - Para IA recomendadora, pode-se precisar de Feature Stores ou *Wide Tables* combinando histórico do usuário + transação baseada não só em dimensões, mas embeddings armazenadas.
