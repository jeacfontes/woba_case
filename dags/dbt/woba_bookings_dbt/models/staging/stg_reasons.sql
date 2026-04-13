{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='reason_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Reasons: lookup simples de motivos de cancelamento.

with cleaned as (
    select
        reason_id,
        reason_description,
        source_created_at
    from {{ ref('clean_reasons') }}
    {{ incremental_filter('source_created_at', 'source_created_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['reason_id']) }} as reason_stg_sk,
    reason_id,
    reason_description,
    source_created_at
from cleaned
