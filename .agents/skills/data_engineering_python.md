---
name: Pipelines Python Ad-hoc
description: Diretrizes de como criar scripts de dados robustos em Python (apoio pipeline).
---

# Skill: Python para Engenharia e Análise de Dados

A parte lógica de automação e modelagem de output deve seguir regras de "Clean Code" durante a escrita de algoritmos auxiliares (ex: o Python script que levanta risco de Churn).

## Regras
1. **Manipulação de Bibliotecas Clássicas**:
   - Utilize a biblioteca que melhor resolve o problema e seja clara para revisão: `pandas` é perfeita para análise descritiva de pequenos volumes ou `csv` base, enquanto que `polars` / `pyarrow` lida bem com perf escaláveis. No limite das necessidades do desafio, `pandas` é perfeitamente validado.

2. **Tipagem e Documentação**:
   - As funções **devem** ser tipadas (`def calcular_churn(df: pd.DataFrame) -> pd.DataFrame:`).
   - Use docstrings e comentários para relatar fluxo lógico (ex: o porquê 30% foi escolhido, por ser exigido no problema `baixo de 30%`).

3. **Arquitetura do Script**:
   - Isole a lógica em passos fáceis:
     - `load_data()`
     - `analyze_utilization()`
     - `export_dataset()`
   - Utilize loggers ao invés de print (ou utilize print mas trate de explicar o output final no README de forma a não perder rastreabilidade).

4. **Design de Entrega**:
   - O output a ser gerado deve se tornar um artefato legível (`json`, `parquet` simples) para garantir ingestão/consumo prático downstream.
