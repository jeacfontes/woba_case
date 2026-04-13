{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_sk',
    on_schema_change='sync_all_columns'
) }}

-- Tabela INFERIDA: lookup de status das reservas.

with source as (
    select * from {{ source('raw_bookings', 'status') }}
    {{ incremental_filter('created_at', 'source_created_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['status_id']) }} as status_sk,

        cast(status_id as bigint)           as status_id,
        cast(status_name as varchar)        as status_name,
        cast(status_description as varchar) as status_description,
        cast(created_at as timestamp)       as source_created_at
    from source
)

select * from cleaned
