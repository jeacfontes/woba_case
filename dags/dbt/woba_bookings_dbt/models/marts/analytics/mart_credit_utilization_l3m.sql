{{ config(
    materialized='table',
    tags=['analytics', 'reporting']
) }}

-- =====================================================================
-- MART: Taxa de Utilização de Créditos por Empresa (Últimos 3 meses)
-- Pergunta de negócio: Qual a taxa de utilização de créditos por empresa
-- nos últimos 3 meses, segmentada por tipo de plano, comparada à média geral?
--
-- Decisões de performance para Athena/Iceberg:
-- - Filtro de data no WHERE para partition pruning no Iceberg
-- - NULLIF para evitar divisão por zero sem try/catch
-- - CROSS JOIN com CTE escalar (1 linha) para evitar subquery correlacionada
-- =====================================================================

with f_bookings as (
    select
        company_id,
        plan_id,
        credits_used
    from {{ ref('fact_bookings') }}
    where check_in_date >= date_add('month', -3, current_date)
      and is_cancelled = false
      and is_no_show = false
),

d_companies as (
    select
        company_id,
        company_name,
        company_size_tier
    from {{ ref('dim_companies') }}
),

d_plans as (
    select
        plan_id,
        plan_name,
        plan_type,
        monthly_credits,
        cost_per_credit
    from {{ ref('dim_plans') }}
),

empresa_aggregated as (
    select
        c.company_id,
        c.company_name,
        c.company_size_tier,
        p.plan_name,
        p.plan_type,
        p.monthly_credits   as creditos_disponiveis_mensais,
        p.cost_per_credit,
        sum(b.credits_used)  as creditos_consumidos_trimestre
    from f_bookings b
    inner join d_companies c on b.company_id = c.company_id
    inner join d_plans p     on b.plan_id    = p.plan_id
    group by 1, 2, 3, 4, 5, 6, 7
),

empresa_utilizacao as (
    select
        company_id,
        company_name,
        company_size_tier,
        plan_name,
        plan_type,
        creditos_consumidos_trimestre,
        (creditos_disponiveis_mensais * 3)  as teto_creditos_trimestre,
        cost_per_credit,

        round(
            100.0 * (
                cast(creditos_consumidos_trimestre as double) /
                nullif(cast((creditos_disponiveis_mensais * 3) as double), 0.0)
            ),
            2
        ) as taxa_utilizacao_perc

    from empresa_aggregated
),

media_geral as (
    select
        avg(taxa_utilizacao_perc) as taxa_utilizacao_media_geral
    from empresa_utilizacao
    where taxa_utilizacao_perc is not null
)

select
    e.company_id,
    e.company_name,
    e.company_size_tier,
    e.plan_name,
    e.plan_type,
    e.creditos_consumidos_trimestre,
    e.teto_creditos_trimestre,
    e.cost_per_credit,
    e.taxa_utilizacao_perc,
    round(g.taxa_utilizacao_media_geral, 2) as media_geral_rede_perc,

    round(e.taxa_utilizacao_perc - g.taxa_utilizacao_media_geral, 2) as delta_vs_media,

    case
        when e.taxa_utilizacao_perc < 30.0                          then 'Crítico (Risco Churn)'
        when e.taxa_utilizacao_perc < g.taxa_utilizacao_media_geral then 'Abaixo da Média'
        else 'Acima da Média'
    end as status_de_engajamento

from empresa_utilizacao e
cross join media_geral g
order by e.taxa_utilizacao_perc asc
