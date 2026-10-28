{% macro normalizar_texto(coluna) -%}
    nullif(upper(regexp_replace(trim({{ coluna }}), '\s+', ' ', 'g')), '')
{%- endmacro %}
