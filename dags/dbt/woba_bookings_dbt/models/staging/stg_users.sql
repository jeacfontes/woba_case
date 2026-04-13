{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_stg_sk',
    on_schema_change='sync_all_columns'
) }}

-- Staging de Users: aplica mascaramento parcial de PII e normaliza dados do colaborador.

with users as (
    select
        user_id,
        full_name,
        email,
        phone,
        is_active,
        source_created_at,
        source_updated_at
    from {{ ref('clean_users') }}
    {{ incremental_filter('source_updated_at', 'source_updated_at') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_id', 'source_updated_at']) }} as user_stg_sk,

        user_id,
        full_name,

        -- Regra de negócio: mascaramento parcial do e-mail para LGPD/compliance
        {{ mask_email('email') }} as email_masked,

        {{ mask_phone('phone') }} as phone_masked,
        is_active,
        source_created_at,
        source_updated_at

    from users
)

select * from enriched
