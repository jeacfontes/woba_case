{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='plan_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Plans: calcula o valor por crédito para análises de custo-benefício.

with plans as (
    select
        plan_id,
        plan_name,
        plan_type,
        monthly_credits,
        plan_value,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('clean_plans') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['plan_id', 'source_updated_at']) }} as plan_stg_sk,

        plan_id,
        plan_name,
        plan_type,
        monthly_credits,
        plan_value,

        -- Regra de negócio: custo unitário por crédito
        case
            when monthly_credits > 0
            then round(cast(plan_value as double) / cast(monthly_credits as double), 2)
            else null
        end as cost_per_credit,

        is_active,
        source_created_at,
        source_updated_at

    from plans
)

select * from enriched
