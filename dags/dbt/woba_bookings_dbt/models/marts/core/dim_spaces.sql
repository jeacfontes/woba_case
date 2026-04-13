{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- =====================================================================
-- DIM_SPACES: Dimensão de espaços com SCD Tipo 2
-- Justificativa: Espaços mudam de tier, capacidade e categoria ao longo
-- do tempo. Precisamos manter o histórico para não distorcer análises
-- retroativas (ex: não re-precificar reservas passadas com tier atual).
-- =====================================================================

with stg_spaces as (
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
        occupancy_rate_pct,
        office_group_name,
        is_active,
        source_updated_at
    from {{ ref('stg_spaces') }}
),

windowed as (
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
        occupancy_rate_pct,
        office_group_name,
        is_active,
        source_updated_at,
        lead(source_updated_at) over (
            partition by space_id
            order by source_updated_at
        ) as next_updated_at
    from stg_spaces
),

scd2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['space_id', 'source_updated_at']) }} as space_dw_sk,

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
        occupancy_rate_pct,
        office_group_name,
        is_active,
        source_updated_at                                              as valid_from,
        coalesce(next_updated_at, cast('9999-12-31' as timestamp))     as valid_to,
        case
            when next_updated_at is null then true
            else false
        end                                                            as is_current_version

    from windowed
)

select
    space_dw_sk,
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
    occupancy_rate_pct,
    office_group_name,
    is_active,
    valid_from,
    valid_to,
    is_current_version
from scd2

{% if is_incremental() %}
    where valid_from > (select max(valid_from) from {{ this }})
{% endif %}
