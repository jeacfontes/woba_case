{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='plan_sk',
    on_schema_change='sync_all_columns'
) }}

with source as (
    select * from {{ source('raw_bookings', 'plans') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_sk,

        cast(plan_id as bigint)             as plan_id,
        cast(plan_name as varchar)          as plan_name,
        cast(plan_type as varchar)          as plan_type,
        cast(monthly_credits as integer)    as monthly_credits,
        cast(plan_value as double)          as plan_value,
        cast(is_active as boolean)          as is_active,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
