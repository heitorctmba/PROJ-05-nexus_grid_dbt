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
                    PERFORM set_config('search_path', '{{ this.schema }}, public', false);
                    PERFORM create_hypertable(
                        '{{ this.identifier }}',
                        'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                    EXECUTE format('ALTER TABLE %I.%I SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = ''device_id'',
                        timescaledb.compress_orderby = ''timestamp DESC''
                    )', '{{ this.schema }}', '{{ this.identifier }}');
                    PERFORM add_compression_policy(
                        format('%I.%I', '{{ this.schema }}', '{{ this.identifier }}')::regclass,
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    );
                    PERFORM add_retention_policy(
                        format('%I.%I', '{{ this.schema }}', '{{ this.identifier }}')::regclass,
                        INTERVAL '1 year',
                        if_not_exists => TRUE
                    );
                END $$;
            {% endif %}
            """
        ],
        tags=['stg']
    )
}}

/*
    ============================================================================
    STAGING MODEL: stg_relay_discrete_data
    ============================================================================

    Descrição:
        Extrai dados discretos (eventos/alarmes) dos relés de proteção em
        formato JSON flexível. Separa campos discretos dos analógicos.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_relay
        Destino: stg_relay_discrete_data (Hypertable TimescaleDB)
        Atualização: Incremental (a cada 5 minutos)

    Estratégia de Dados:
        - Armazena campos discretos de proteção em formato JSONB
        - Exclui campos analógicos (já mapeados em stg_relay_analogic)
        - Parsing e join com catálogo será feito em camada INTERMEDIATE

    Campos Discretos dos Relés:
        - overvoltage, undervoltage
        - time_overcurrent, instantaneous_overcurrent
        - neutral_time_overcurrent, neutral_instantaneous_overcurrent
        - current_imbalance, voltage_imbalance
        - directional_power
        - overfrequency_protection, underfrequency_protection
        - breaker_position
        - communication_failure

    Características:
        - Hypertable com chunks de 1 dia
        - Compressão automática após 7 dias
        - Retenção de 1 ano
        - Unique key: (device_id, timestamp)

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-23
    ============================================================================
*/

WITH source_data AS (
    SELECT
        timestamp AS raw_timestamp,
        power_plant_id,
        device_id,
        json_data
    FROM {{ source('raw', 'raw_relay') }}
    WHERE json_data->>'timestamp' IS NOT NULL

    {% if is_incremental() %}
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

-- Lista de campos analógicos que devem ser excluídos
excluded_fields AS (
    SELECT unnest(ARRAY[
        'timestamp',
        'active_power',
        'apparent_power',
        'power_reactive',
        'current_phase_a',
        'current_phase_b',
        'current_phase_c',
        'line_voltage_ab',
        'line_voltage_bc',
        'line_voltage_ca'
    ]) AS field_name
),

-- Extrair apenas campos discretos (alarmes/eventos)
extracted_discrete AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,

        -- Filtrar JSON para conter apenas campos discretos
        (
            SELECT jsonb_object_agg(key, value)
            FROM jsonb_each(json_data)
            WHERE
                -- Excluir campos analógicos
                key NOT IN (SELECT field_name FROM excluded_fields)
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
WHERE discrete_data_json IS NOT NULL
