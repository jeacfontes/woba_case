{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='group_sk',
    on_schema_change='sync_all_columns'
) }}

-- Tabela INFERIDA: agrupamento lógico de escritórios/espaços parceiros.

with source as (
    select * from {{ source('raw_bookings', 'office_groups') }}
    {{ incremental_filter('updated_at', 'source_updated_at') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['group_id']) }} as group_sk,

        cast(group_id as bigint)            as group_id,
        cast(group_name as varchar)         as group_name,
        cast(created_at as timestamp)       as source_created_at,
        cast(updated_at as timestamp)       as source_updated_at
    from source
)

select * from cleaned
