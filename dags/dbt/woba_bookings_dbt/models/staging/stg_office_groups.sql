{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='group_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Office Groups: lookup simples.

with cleaned as (
    select
        group_id,
        group_name,
        source_created_at,
        source_updated_at
    from {{ ref('clean_office_groups') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['group_id']) }} as group_stg_sk,
    group_id,
    group_name,
    source_created_at,
    source_updated_at
from cleaned
