{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Bookings: enriquece a reserva com regras de negócio e desnormaliza lookups essenciais.

with bookings as (
    select
        booking_id,
        date_id,
        space_id,
        user_id,
        company_id,
        group_id,
        plan_id,
        booking_type_id,
        status_id,
        credits,
        credits_in_money,
        check_in_datetime,
        check_out_datetime,
        check_in_latitude,
        check_in_longitude,
        is_cancelled,
        is_no_show,
        is_future,
        app_type,
        source_created_at,
        source_updated_at
    from {{ ref('clean_bookings') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
),

status_lookup as (
    select
        status_id,
        status_name
    from {{ ref('clean_status') }}
),

booking_types_lookup as (
    select
        booking_type_id,
        type_name
    from {{ ref('clean_booking_types') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.booking_id', 'b.source_updated_at']) }} as booking_stg_sk,

        b.booking_id,
        b.date_id,
        b.space_id,
        b.user_id,
        b.company_id,
        b.group_id,
        b.plan_id,
        b.booking_type_id,
        b.status_id,

        -- Desnormalização de lookups para evitar joins repetitivos no DW
        s.status_name,
        bt.type_name as booking_type_name,

        -- Métricas brutas
        b.credits,
        b.credits_in_money,
        {{ revenue_to_reais('b.credits_in_money') }} as revenue_brl,

        -- Timestamps
        b.check_in_datetime,
        b.check_out_datetime,
        date(b.check_in_datetime) as check_in_date,

        -- Duração calculada (regra de negócio)
        date_diff(
            'minute',
            b.check_in_datetime,
            b.check_out_datetime
        ) as duration_minutes,

        -- Geolocalização
        b.check_in_latitude,
        b.check_in_longitude,

        -- Flags de estado
        b.is_cancelled,
        b.is_no_show,
        b.is_future,
        b.app_type,

        -- Regra de negócio: status derivado consolidado
        case
            when b.is_cancelled = true  then 'cancelled'
            when b.is_no_show = true    then 'no_show'
            when b.is_future = true     then 'scheduled'
            when b.check_in_datetime is not null
             and b.check_out_datetime is not null then 'completed'
            when b.check_in_datetime is not null  then 'checked_in'
            else 'unknown'
        end as booking_status_derived,

        -- Metadados
        b.source_created_at,
        b.source_updated_at

    from bookings b
    left join status_lookup s    on b.status_id = s.status_id
    left join booking_types_lookup bt on b.booking_type_id = bt.booking_type_id
)

select * from enriched
