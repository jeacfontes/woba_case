{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_dw_sk',
    on_schema_change='sync_all_columns'
) }}

-- =====================================================================
-- DIM_USERS: Dimensão de usuários/colaboradores — SCD Type 1
-- Tabela INFERIDA. Dados sensíveis já mascarados na camada Staging.
-- =====================================================================

with stg_users as (
    select
        user_id,
        full_name,
        email_masked,
        phone_masked,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('stg_users') }}
    {{ incremental_filter('source_updated_at', 'updated_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_dw_sk,
    user_id,
    full_name,
    email_masked,
    phone_masked,
    is_active,
    source_created_at       as created_at,
    source_updated_at       as updated_at
from stg_users
