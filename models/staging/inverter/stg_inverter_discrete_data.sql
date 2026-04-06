{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        indexes=[
            {'columns': ['device_id', 'timestamp'], 'type': 'btree'},
            {'columns': ['power_plant_id', 'timestamp'], 'type': 'btree'}
        ],
        pre_hook=[
            "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"
        ],
        post_hook=[
            """
            {% if not is_incremental() %}
                DO $$
                BEGIN
                    -- Configurar search_path para incluir o schema da tabela
                    PERFORM set_config('search_path', '{{ this.schema }}, public', false);

                    -- Criar hypertable usando apenas o identifier (schema já está no search_path)
                    PERFORM create_hypertable(
                        '{{ this.identifier }}',
                        'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );

                    -- Configurar compressão automática
                    EXECUTE format('ALTER TABLE %I.%I SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = ''device_id'',
                        timescaledb.compress_orderby = ''timestamp DESC''
                    )', '{{ this.schema }}', '{{ this.identifier }}');

                    -- Adicionar política de compressão (após 7 dias) - usar nome qualificado
                    PERFORM add_compression_policy(
                        format('%I.%I', '{{ this.schema }}', '{{ this.identifier }}')::regclass,
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    );

                    -- Adicionar política de retenção (1 ano) - usar nome qualificado
                    PERFORM add_retention_policy(
                        format('%I.%I', '{{ this.schema }}', '{{ this.identifier }}')::regclass,
                        INTERVAL '1 year',
                        if_not_exists => TRUE
                    );
                END $$;
            {% endif %}
            """
        ]
    )
}}

/*
    ============================================================================
    STAGING MODEL: stg_inverter_discrete_data
    ============================================================================

    Descrição:
        Extrai dados discretos (eventos/alarmes) dos inversores em formato JSON
        flexível. Esta abordagem permite receber qualquer código discreto sem
        necessidade de mapear colunas fixas.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_inverter
        Destino: stg_inverter_discrete_data (Hypertable TimescaleDB)
        Atualização: Incremental (a cada 5 minutos)

    Estratégia de Dados:
        - Armazena apenas códigos numéricos discretos em formato JSONB
        - Exclui campos analógicos (já mapeados em stg_inverter_analogic)
        - Exclui campos que começam com "str" (são strings)
        - Parsing e join com catálogo será feito em camada INTERMEDIATE

    Características:
        - Hypertable com chunks de 1 dia
        - Compressão automática após 7 dias (JSONB comprime muito bem)
        - Retenção de 1 ano
        - Unique key: (device_id, timestamp)
        - JSONB permite indexação GIN para queries rápidas

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-22
    ============================================================================
*/

WITH source_data AS (
    SELECT
        timestamp AS raw_timestamp,
        power_plant_id,
        device_id,
        json_data
    FROM {{ source('raw', 'raw_inverter') }}
    WHERE json_data->>'timestamp' IS NOT NULL

    {% if is_incremental() %}
        -- Processar apenas dados novos (incrementais)
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

-- Lista de campos analógicos que devem ser excluídos (já estão em stg_inverter_analogic)
excluded_fields AS (
    SELECT unnest(ARRAY[
        'timestamp',
        'active_power',
        'power_reactive',
        'power_input',
        'power_factor',
        'daily_active_energy',
        'cumulative_active_energy',
        'frequency',
        'efficiency',
        'current_phase_a',
        'current_phase_b',
        'current_phase_c',
        'line_voltage_ab',
        'line_voltage_bc',
        'line_voltage_ca',
        'string_voltage',
        'temperature_internal',
        'resistance_insulation',
        'state_operation',
        'state_simplified'
    ]) AS field_name
),

-- Extrair apenas campos discretos (códigos numéricos e communication_failure)
extracted_discrete AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,

        -- Filtrar JSON para conter apenas campos discretos (códigos numéricos)
        -- Remove campos analógicos e campos que começam com "str" (strings)
        (
            SELECT jsonb_object_agg(key, value)
            FROM jsonb_each(json_data)
            WHERE
                -- Incluir apenas campos numéricos (códigos) ou communication_failure
                (key ~ '^\d+$' OR key = 'communication_failure')
                -- Excluir campos analógicos
                AND key NOT IN (SELECT field_name FROM excluded_fields)
        ) AS discrete_data_json

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC
)

SELECT
    timestamp,
    power_plant_id,
    device_id,
    discrete_data_json
FROM extracted_discrete
WHERE discrete_data_json IS NOT NULL  -- Apenas registros com dados discretos
