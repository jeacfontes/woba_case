---
name: Padrões de Qualidade e Práticas de dbt
description: Convenções de nomenclatura, testes e materialização no dbt para ambiente Athena/Iceberg.
---

# Skill: Boas Práticas dbt

Essas práticas deverão ser seguidas no momento de desenhar os scripts SQL do ecossistema dbt do desafio Woba.

## 1. Nomenclatura Padrão
- Fatos (`fact_`): Devem ser nomeadas no plural e representar transações/eventos métricos (ex: `fact_bookings`).
- Dimensões (`dim_`): Entidades de negócio ricas no modelo estrela (ex: `dim_spaces`, `dim_companies`).
- Staging (`stg_`): Limpeza 1-1 em base (ex: `stg_mysql_application__bookings`).

## 2. Materialização Incremental (Trino/Athena com Apache Iceberg)
- Iceberg suporta atomic transactions. Dessa forma, opte por materialização `incremental` definindo uma estratégia `merge`.
- Exemplo de conf em um modelo:
  ```jinja
  {{ config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='booking_id',
      on_schema_change='sync_all_columns'
  ) }}
  ```

## 3. Gestão e Cultura de Testes
- Todos os primary keys devem conter os testes `unique` e `not_null`.
- Teste campos como `status_id` via array de `accepted_values`.
- É **obrigatório** ter testes singulares. Eles devem ficar em `/tests/`. Exemplo requerido no desafio: Garantir lógicamente que cancelamentos e check-ins não sejam excludentes/ilegais ("reservas canceladas sem check-in registrado").

## 4. Uso de Macros
- Macros devem isolar as regras de abstração de negócio complexas como conversões de tempo global (ex: padronização de timezone `timestamp_tz_to_utc()`) ou conversão monetária padrão em faturamento.
