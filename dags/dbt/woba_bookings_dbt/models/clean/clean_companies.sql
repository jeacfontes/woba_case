{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='company_sk',
    on_schema_change='sync_all_columns'
) }}

with source as (
    select * from {{ source('raw_bookings', 'companies') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_sk,

        cast(company_id as bigint)          as company_id,
        cast(company_name as varchar)       as company_name,
        cast(industry as varchar)           as industry,
        cast(employee_count as integer)     as employee_count,
        cast(is_active as boolean)          as is_active,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
