{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_dw_sk',
    on_schema_change='sync_all_columns'
) }}

-- =====================================================================
-- FACT_BOOKINGS: Tabela fato principal do domínio Bookings
-- Grain: uma linha por reserva individual (booking_id)
-- Consome da camada Staging que já aplicou regras de negócio e lookups
-- =====================================================================

with stg_bookings as (
    select
        booking_id,
        date_id,
        space_id,
        user_id,
        company_id,
        plan_id,
        group_id,
        booking_type_id,
        status_id,
        booking_type_name,
        status_name,
        credits,
        credits_in_money,
        revenue_brl,
        duration_minutes,
        check_in_datetime,
        check_out_datetime,
        check_in_date,
        check_in_latitude,
        check_in_longitude,
        is_cancelled,
        is_no_show,
        is_future,
        app_type,
        booking_status_derived,
        source_created_at,
        source_updated_at
    from {{ ref('stg_bookings') }}
    {{ incremental_filter('source_updated_at', 'updated_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['booking_id']) }} as booking_dw_sk,

    booking_id,
    date_id,
    space_id,
    user_id,
    company_id,
    plan_id,
    group_id,
    booking_type_id,
    status_id,
    booking_type_name,
    status_name,
    credits                     as credits_used,
    credits_in_money,
    revenue_brl,
    duration_minutes,
    check_in_datetime,
    check_out_datetime,
    check_in_date,
    check_in_latitude,
    check_in_longitude,
    is_cancelled,
    is_no_show,
    is_future,
    app_type,
    booking_status_derived,
    source_created_at           as created_at,
    source_updated_at           as updated_at

from stg_bookings
