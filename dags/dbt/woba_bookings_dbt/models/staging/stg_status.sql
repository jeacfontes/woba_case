{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Status: lookup simples.

with cleaned as (
    select
        status_id,
        status_name,
        status_description,
        source_created_at
    from {{ ref('clean_status') }}
    {{ incremental_filter('source_created_at', 'source_created_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['status_id']) }} as status_stg_sk,
    status_id,
    status_name,
    status_description,
    source_created_at
from cleaned
