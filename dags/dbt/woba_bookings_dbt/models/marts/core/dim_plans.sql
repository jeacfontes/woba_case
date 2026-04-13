{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='plan_dw_sk',
    on_schema_change='sync_all_columns'
) }}

-- =====================================================================
-- DIM_PLANS: Dimensão de planos de assinatura — SCD Type 1
-- =====================================================================

with stg_plans as (
    select
        plan_id,
        plan_name,
        plan_type,
        monthly_credits,
        plan_value,
        cost_per_credit,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('stg_plans') }}
    {{ incremental_filter('source_updated_at', 'updated_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_dw_sk,
    plan_id,
    plan_name,
    plan_type,
    monthly_credits,
    plan_value,
    cost_per_credit,
    is_active,
    source_created_at       as created_at,
    source_updated_at       as updated_at
from stg_plans
