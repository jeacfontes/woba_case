---
description: Parte 4 — Bônus (peso 10%)
---

# Parte 4: Bônus

## Objetivo
Escolher **UMA** das opções abaixo de engenharia e descrever profundamente ou implementar. Escreva as respostas e fluxos no README.

## Opção A: Observabilidade
- Desenhe um pipeline para acompanhar métricas de engenharia (freshness, volume de dados, mudanças bruscas em metadata/schema, falhas de testes).
- Responda como irá monitorar, e como deve ser feito o disparo de alertas e quem consome isso.

## Opção B: CI/CD
- Descreva detalhadamente ou implemente (ex: via Github Actions) um fluxo de Continuous Integration/Continuous Deployment para o dbt.
- Explique as actions: O que roda no PR? O que roda no momento do merge para o branch main?
- Como proteger a branch de relatórios (main) de forma que dados quebrados não sejam materializados no ambiente produtivo de Athena/BI?
