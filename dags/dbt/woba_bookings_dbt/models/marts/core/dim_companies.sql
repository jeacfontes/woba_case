{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='company_dw_sk',
    on_schema_change='sync_all_columns'
) }}

-- =====================================================================
-- DIM_COMPANIES: Dimensão de empresas parceiras — SCD Type 1
-- =====================================================================

with stg_companies as (
    select
        company_id,
        company_name,
        industry,
        employee_count,
        company_size_tier,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('stg_companies') }}
    {{ incremental_filter('source_updated_at', 'updated_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_dw_sk,
    company_id,
    company_name,
    industry,
    employee_count,
    company_size_tier,
    is_active,
    source_created_at       as created_at,
    source_updated_at       as updated_at
from stg_companies
