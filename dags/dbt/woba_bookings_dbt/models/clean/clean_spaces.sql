{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='space_sk',
    on_schema_change='sync_all_columns'
) }}

with source as (
    select * from {{ source('raw_bookings', 'spaces') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['space_id']) }} as space_sk,

        cast(space_id as bigint)            as space_id,
        cast(space_name as varchar)         as space_name,
        cast(vanity_name as varchar)        as vanity_name,
        cast(country as varchar)            as country,
        cast(city as varchar)               as city,
        cast(neighborhood as varchar)       as neighborhood,
        cast(address as varchar)            as address,
        cast(latitude as double)            as latitude,
        cast(longitude as double)           as longitude,
        cast(timezone as varchar)           as timezone,
        cast(space_type as varchar)         as space_type,
        cast(category as varchar)           as category,
        cast(level as varchar)              as level,
        cast(tier as varchar)               as tier,
        cast(total_seats as integer)        as total_seats,
        cast(available_seats as integer)    as available_seats,
        cast(total_private_rooms as integer) as total_private_rooms,
        cast(is_active as boolean)          as is_active,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
