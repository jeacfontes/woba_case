{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='company_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Companies: classifica o porte da empresa com base na contagem de funcionários.

with companies as (
    select
        company_id,
        company_name,
        industry,
        employee_count,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('clean_companies') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_id', 'source_updated_at']) }} as company_stg_sk,

        company_id,
        company_name,
        industry,
        employee_count,

        -- Regra de negócio: classificação por porte
        case
            when employee_count <= 50   then 'Small'
            when employee_count <= 200  then 'Medium'
            when employee_count <= 1000 then 'Large'
            else 'Enterprise'
        end as company_size_tier,

        is_active,
        source_created_at,
        source_updated_at

    from companies
)

select * from enriched
