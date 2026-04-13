{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='space_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Spaces: enriquece com grupo de escritório e calcula métricas de capacidade.

with spaces as (
    select
        space_id,
        space_name,
        vanity_name,
        country,
        city,
        neighborhood,
        address,
        latitude,
        longitude,
        timezone,
        space_type,
        category,
        level,
        tier,
        total_seats,
        available_seats,
        total_private_rooms,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('clean_spaces') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
),

office_groups as (
    select
        group_id,
        group_name
    from {{ ref('clean_office_groups') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['sp.space_id', 'sp.source_updated_at']) }} as space_stg_sk,

        sp.space_id,
        sp.space_name,
        sp.vanity_name,

        -- Localização
        sp.country,
        sp.city,
        sp.neighborhood,
        sp.address,
        sp.latitude,
        sp.longitude,
        sp.timezone,

        -- Classificação
        sp.space_type,
        sp.category,
        sp.level,
        sp.tier,

        -- Capacidade
        sp.total_seats,
        sp.available_seats,
        sp.total_private_rooms,

        -- Regra de negócio: taxa de ocupação potencial do espaço
        case
            when sp.total_seats > 0
            then round(
                100.0 * (cast(sp.total_seats - sp.available_seats as double) / cast(sp.total_seats as double)),
                2
            )
            else 0.0
        end as occupancy_rate_pct,

        -- Enriquecimento com grupo
        og.group_name as office_group_name,

        sp.is_active,
        sp.source_created_at,
        sp.source_updated_at

    from spaces sp
    left join office_groups og on sp.space_id = og.group_id
)

select * from enriched
