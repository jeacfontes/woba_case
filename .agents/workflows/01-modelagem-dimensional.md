---
description: Parte 1 — Modelagem Dimensional (peso 35%)
---

# Parte 1: Modelagem Dimensional

## Objetivo
Criar a camada semântica do domínio "Bookings", alimentando o BI, análises ad-hoc e futuros agentes de IA.

## Passos para Execução

1. **Desenho do Modelo (Diagrama)**
   - Crie um diagrama do modelo dimensional. Pode usar Mermaid no README ou draw.io exportado para a pasta `/docs`.
   - Identifique **tabelas fato** e **tabelas dimensão** (mesmo as inferidas).
   - Defina claramente o **grain (granularidade)** de cada fato.
   - Defina os **relacionamentos**.
   - Defina o **SCD (Slowly Changing Dimension)** para cada dimensão com justificativa (ex: espaços mudam de tier/capacidade).

2. **Modelagem SQL (dbt)**
   - Crie os scripts SQL dos modelos dbt em `/dbt_project/models/`:
     - `fact_bookings`: fato principal.
     - `dim_spaces`: dimensão de espaços com tratamento de SCD histórico.
     - `dim_companies`: dimensão de empresas.
   - Organize em camadas (ex: `staging` -> `marts`/`DW`) e anote a justificativa dessa arquitetura.
   
3. **Mapeamento de Lacunas de Informação**
   - Escreva no arquivo README as conclusões sobre quais informações ou tabelas precisariam ser solicitadas ao time.
   - Descreva como conduziria essa conversa com os stakeholders.

4. **Impacto de Casos de Uso Avançados (IA)**
   - Responda (no README): Se a Woba quisesse criar um agente de IA para recomendar espaços com base no histórico, como isso impactaria as decisões de modelagem dimensional tomadas acima?
