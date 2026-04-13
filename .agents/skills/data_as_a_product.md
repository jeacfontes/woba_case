---
name: Data as a Product e Colaboração
description: Como estruturar as respostas voltadas à área de Negócios e Produto.
---

# Skill: Entregáveis Discursivos e de Produto

Como Analytics Engineer na modern data stack, a visão deve ser orientada na gestão dos dados como Produto (Data Mesh/Data as a Product). Use esse Mindset ao responder o Teste.

## 1. Lidando Com Ambiguidade
O analista não deve assumir respostas às escuras. Ao lidar com um stakeholder exigindo dashboards genéricos até na semana que vem:
- **Priorize O Negócio em 1-pager**: Qual é a tese de negócio que o KPI precisa responder? Que alavanca vão acionar ao verem que a métrica caiu?
- **Scope e Expectativas**: Não responda "ok" nem "impossível". Fale sobre viabilidade ágil, entregando MVT (Minimum Viable Table) com os KPIs primordiais primeiro.
- **Riscos de SLA**: Se os dags da fonte quebrar, o BI precisa alertar.

## 2. Contratos de Dados (Data Contracts)
Se vamos exportar a recomendação de reservas para o Produto, devemos gerir expectativas. Use schema validation forte, API contracts, limites de tolerância de Freshness, Ownership, SLAs de falhas e políticas de expiração de dados se houver PI (dados pessoais).

## 3. Real-time vs Batch Processing
Seja pragmático quanto a custos arquiteturais de Nuvem. Exija Real-time (que é mais complexo em Athena do que num broker Kafka/Kinesis + Flink) apenas se houver necessidade imperativa em UX do Produto. Em reservas, o batch por hora é na verdade um _micro-batching_ e resolve grande área da precisão exigida, custando centenas de vezes menos que streaming sem perda grave para negócios B2B.
