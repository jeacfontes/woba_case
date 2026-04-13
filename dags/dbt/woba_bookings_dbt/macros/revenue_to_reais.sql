{% macro revenue_to_reais(column_name) -%}
    {# Converte o valor de centavos local do banco de dados (int) para representação monetária fina usando Trino #}
    round( cast({{ column_name }} as double) / 100.0, 2 )
{%- endmacro %}
