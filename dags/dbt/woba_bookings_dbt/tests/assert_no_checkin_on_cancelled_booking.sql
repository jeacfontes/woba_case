-- Objetivo: Avaliar Qualidade Lógica. Nenhuma reserva marcada como cancelada pode contar com tempo de check-in registrado.

select
    booking_id,
    check_in_datetime,
    is_cancelled
from {{ ref('fact_bookings') }}
where is_cancelled = true
  and check_in_datetime is not null
