{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_sk',
    on_schema_change='sync_all_columns'
) }}

with source as (
    select * from {{ source('raw_bookings', 'bookings') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        -- Surrogate key para garantir unicidade e rastreabilidade entre camadas
        {{ dbt_utils.generate_surrogate_key(['booking_id']) }} as booking_sk,

        cast(booking_id as bigint)          as booking_id,
        cast(date_id as bigint)             as date_id,
        cast(space_id as bigint)            as space_id,
        cast(user_id as bigint)             as user_id,
        cast(company_id as bigint)          as company_id,
        cast(group_id as bigint)            as group_id,
        cast(plan_id as bigint)             as plan_id,
        cast(booking_type_id as bigint)     as booking_type_id,
        cast(status_id as bigint)           as status_id,
        cast(credits as double)             as credits,
        cast(credits_in_money as double)    as credits_in_money,
        cast(check_in_datetime as timestamp)  as check_in_datetime,
        cast(check_out_datetime as timestamp) as check_out_datetime,
        cast(check_in_latitude as double)   as check_in_latitude,
        cast(check_in_longitude as double)  as check_in_longitude,
        cast(is_cancelled as boolean)       as is_cancelled,
        cast(is_no_show as boolean)         as is_no_show,
        cast(is_future as boolean)          as is_future,
        cast(app_type as varchar)           as app_type,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
