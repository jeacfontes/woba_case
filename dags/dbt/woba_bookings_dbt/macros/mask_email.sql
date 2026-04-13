{% macro mask_email(column_name) -%}
    {#
        Macro para mascaramento parcial de e-mail em conformidade com LGPD (Lei 13.709/2018).

        Por que aplicar na camada de transformação?
        - A Woba opera com dados de colaboradores de empresas clientes (B2B).
          Expor e-mails completos em camadas analíticas (BI, exports, feature stores)
          viola o princípio de minimização da LGPD (Art. 6º, III).
        - Aplicar o mascaramento já na staging garante que nenhum consumidor
          downstream (Power BI, analistas ad-hoc, modelos de IA) tenha acesso
          ao dado pessoal completo, sem depender de controles de acesso no BI.
        - O dado mascarado ainda permite identificação parcial para debugging
          e reconciliação (ex: "edu***@woba.com.br" é suficiente para saber
          de quem se trata sem expor o endereço completo).

        Exemplo:
            "eduardo.fontes@woba.com.br" → "edu***@woba.com.br"

        Args:
            column_name: nome da coluna que contém o e-mail (ex: 'email')
    #}
    concat(
        substr({{ column_name }}, 1, 3),
        '***@',
        split_part({{ column_name }}, '@', 2)
    )
{%- endmacro %}
