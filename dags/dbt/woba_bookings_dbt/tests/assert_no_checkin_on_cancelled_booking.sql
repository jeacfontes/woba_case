-- Teste de Regra de Negócio Customizado (Singular Test)
-- Se uma reserva foi cancelada (is_cancelled = true) E NÃO É NO SHOW,
-- ela teoricamente não deveria ter um horário de check_in registrado.
-- Se retornar alguma linha, o teste falha no dbt!

select
    booking_id,
    is_cancelled,
    is_no_show,
    check_in_datetime
from {{ ref('stg_bookings') }}
where 
    is_cancelled = true
    and is_no_show = false
    and check_in_datetime is not null
