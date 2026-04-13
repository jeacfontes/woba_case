{% macro mask_phone(column_name) -%}
    {#
        Macro para mascaramento parcial de telefone em conformidade com LGPD (Lei 13.709/2018).

        Mesma justificativa da mask_email: o número de telefone é dado pessoal
        sensível e não deve ser exposto por completo em camadas analíticas.
        Mantemos apenas os 4 últimos dígitos para permitir reconciliação
        sem expor o número completo.

        Exemplo:
            "+5511998765432" → "***5432"
            "11998765432"    → "***5432"

        Args:
            column_name: nome da coluna que contém o telefone (ex: 'phone')
    #}
    concat('***', substr({{ column_name }}, length({{ column_name }}) - 3, 4))
{%- endmacro %}
