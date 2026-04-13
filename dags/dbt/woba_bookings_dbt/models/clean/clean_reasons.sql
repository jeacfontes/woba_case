{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='reason_sk',
    on_schema_change='sync_all_columns'
) }}

-- Tabela INFERIDA: motivos de cancelamento ou alteração de reservas.

with source as (
    select * from {{ source('raw_bookings', 'reasons') }}
    {{ incremental_filter('created_at', 'source_created_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['reason_id']) }} as reason_sk,

        cast(reason_id as bigint)               as reason_id,
        cast(reason_description as varchar)     as reason_description,
        cast(created_at as timestamp)           as source_created_at
    from source
)

select * from cleaned
