{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_type_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Booking Types: lookup simples.

with cleaned as (
    select
        booking_type_id,
        type_name,
        is_active,
        source_created_at
    from {{ ref('clean_booking_types') }}
    {{ incremental_filter('source_created_at', 'source_created_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['booking_type_id']) }} as booking_type_stg_sk,
    booking_type_id,
    type_name,
    is_active,
    source_created_at
from cleaned
