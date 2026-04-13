---
description: Parte 2 — dbt na Prática (peso 35%)
---

# Parte 2: dbt na Prática

## Objetivo
Implementar a prática do dbt considerando governança, testes e queries analíticas de qualidade.

## Passos para Execução

1. **Estruturar Projeto dbt**
   - Inicialize o projeto dbt (se aplicável) ou crie as pastas: `models/`, `macros/`, `tests/` etc.
   - Configure o `schema.yml` para documentar os modelos criados na Parte 1 e as colunas principais.
   - Crie pelo menos **1 macro customizada** que seja útil ao contexto.
   - Defina configurações de materialização apropriadas (`incremental` usando estratégias como merge) e as justifique (lembrando do uso de Athena/Iceberg).

2. **Configuração de Testes de Qualidade (Data Quality)**
   - Implemente testes genéricos no `schema.yml`: `unique`, `not_null`, `accepted_values`, `relationships`.
   - Crie pelo menos **1 teste singular (custom)** (na pasta `tests/`). Exemplo: "reservas canceladas não devem ter check-in registrado".
   - Explique (no README) como monitoraria a execução desses testes em produção.

3. **Query Analítica (Business Case)**
   - Escreva uma query SQL analítica forte formatada como modelo mart ou exposure para responder:
     *Qual a taxa de utilização de créditos por empresa nos últimos 3 meses, segmentada por tipo de plano, e como ela se compara com a média geral?*
   - Assegure-se de que é "production-ready" e performática.

4. **Script Python de Apoio a Pipeline**
   - Na raiz do repositório, crie um script Python (`churn_analysis.py`).
   - O script deve "ler" o resultado da query acima (você pode mockar dados via CSV ou JSON).
   - Identifique empresas com taxa inferior a 30% e exporte para outro dataset (estruturado como CSV ou JSON).
   - Garanta aplicação de boas práticas e clareza no código.
