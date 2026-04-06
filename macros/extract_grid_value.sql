{#
    Macro para extrair e converter valores do JSON para tipos específicos

    Suporta dois formatos:
    - Formato legado: "valor@timestamp" (ex: "60.07@2025-12-11T13:50:14Z")
    - Formato atual: valor direto (ex: 60.07 ou "60.07")

    Exemplos:
        - "-517996.544@20251130135055" -> -517996.544 (FLOAT)
        - "-517996.544" -> -517996.544 (FLOAT)
        - 60.01 -> 60.01 (FLOAT)
        - "512.0" -> 512 (INTEGER)
        - 512 -> 512 (INTEGER)

    Uso:
        {{ extract_grid_value("json_data", "active_power") }}
        {{ extract_grid_value("json_data", "state_operation", "INTEGER") }}

    Retorna NULL se:
        - O campo não existir no JSON
        - O valor for NULL ou vazio
#}

{% macro extract_grid_value(json_column, field_name, cast_as='FLOAT') %}
    {%- set numeric_integer_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'INT', 'INT2', 'INT4', 'INT8'] -%}
    {%- if cast_as.upper() in numeric_integer_types -%}
    -- Para tipos inteiros: converte via NUMERIC para aceitar valores como "512.0"
    CAST(
        CAST(
            NULLIF(
                NULLIF(
                    SPLIT_PART(
                        COALESCE({{ json_column }} ->> '{{ field_name }}', ''),
                        '@',
                        1
                    ),
                    ''
                ),
                '-'
            ) AS NUMERIC
        ) AS {{ cast_as }}
    )
    {%- else -%}
    -- Para outros tipos: conversão direta (compatível com ambos os formatos)
    -- Trata valores inválidos como "-" convertendo para NULL
    CAST(
        NULLIF(
            NULLIF(
                SPLIT_PART(
                    COALESCE({{ json_column }} ->> '{{ field_name }}', ''),
                    '@',
                    1
                ),
                ''
            ),
            '-'
        ) AS {{ cast_as }}
    )
    {%- endif -%}
{% endmacro %}
