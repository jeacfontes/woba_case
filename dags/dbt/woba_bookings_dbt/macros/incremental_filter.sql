{% macro incremental_filter(timestamp_column, target_column) %}
    {#
        Macro para filtro incremental padronizado na camada Clean.
        Considerando que os dados raw já chegam incrementais via Airbyte,
        filtramos apenas os registros novos/atualizados desde o último load.
        
        Args:
            timestamp_column: coluna de timestamp na source (ex: 'updated_at', 'created_at')
            target_column: coluna correspondente na tabela destino (ex: 'source_updated_at')
    #}
    {% if is_incremental() %}
        where {{ timestamp_column }} > (
            select coalesce(max({{ target_column }}), cast('1900-01-01' as timestamp))
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
