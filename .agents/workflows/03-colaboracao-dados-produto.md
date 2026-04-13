---
description: Parte 3 — Colaboração e Dados como Produto (peso 20%)
---

# Parte 3: Colaboração e Dados como Produto

## Objetivo
Responder de forma estruturada a cenários de negócio envolvendo os dados analisados e governados. O output deve estar no arquivo `README` do projeto.

## Passos para Execução (Respostas Discursivas)

### 1. Responder ao Cenário A:
**Demanda**: "Preciso de um dashboard para acompanhar a performance dos espaços parceiros. Consegue montar até semana que vem?"
- **Abordagem**: Descreva como estruturar essa demanda, as perguntas a serem feitas e os riscos associados com a data curta e definição ambígua.
- **Data Products**: Proponha quais datasets deverão alimentar esse dashboard. Quais as métricas principais? (ocupação, NPS, receita, etc.) Quais modelos no dbt precisam ser ajustados/criados? Como garantir atualização contínua e confiável (data freshness/reliability)?

### 2. Responder ao Cenário B:
**Demanda**: Feature de produto para recomendar espaços baseada no histórico de reservas (IA ou regras de negócio).
- **Estruturação**: O que muda na modelagem atual para atender Produto e não apenas BI?
- **Data Contracts**: Defina quais contratos de dados seriam firmados entre a camada analítica e os produtos.
- **Trade-offs de Arquitetura**: Dados batch vs near real-time. Como solucionar o impacto arquitetural e financeiro da escolha em data analytics?
