{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_type_sk',
    on_schema_change='sync_all_columns'
) }}

-- Tabela INFERIDA: tipos de reserva (Individual, Meeting Room, etc.)

with source as (
    select * from {{ source('raw_bookings', 'booking_types') }}
    {{ incremental_filter('created_at', 'source_created_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['booking_type_id']) }} as booking_type_sk,

        cast(booking_type_id as bigint)     as booking_type_id,
        cast(type_name as varchar)          as type_name,
        cast(is_active as boolean)          as is_active,
        cast(created_at as timestamp)       as source_created_at
    from source
)

select * from cleaned
