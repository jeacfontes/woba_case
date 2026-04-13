{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_sk',
    on_schema_change='sync_all_columns'
) }}

-- Tabela INFERIDA: não fornecida no teste, mas necessária para completar o modelo dimensional.

with source as (
    select * from {{ source('raw_bookings', 'users') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,

        cast(user_id as bigint)             as user_id,
        cast(full_name as varchar)          as full_name,
        cast(email as varchar)              as email,
        cast(phone as varchar)              as phone,
        cast(is_active as boolean)          as is_active,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
