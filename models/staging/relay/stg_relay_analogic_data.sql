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
        ]
    )
}}

/*
    ============================================================================
    STAGING MODEL: stg_relay_analogic_data
    ============================================================================

    Descrição:
        Extrai e normaliza dados analógicos dos relés de proteção (DJMT)
        a partir da tabela raw_relay. Converte valores do formato Grid Co
        (valor@timestamp) para valores numéricos puros.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_relay
        Destino: stg_relay_analogic_data (Hypertable TimescaleDB)
        Atualização: Incremental (a cada 5 minutos)

    Características:
        - Hypertable com chunks de 1 dia
        - Compressão automática após 7 dias
        - Retenção de 1 ano
        - Unique key: (device_id, timestamp)

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
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

extracted_analogic AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,

        {{ extract_grid_value('json_data', 'active_power') }} AS active_power,
        {{ extract_grid_value('json_data', 'line_voltage_ab') }} AS line_voltage_ab,
        {{ extract_grid_value('json_data', 'line_voltage_bc') }} AS line_voltage_bc,
        {{ extract_grid_value('json_data', 'line_voltage_ca') }} AS line_voltage_ca,
        {{ extract_grid_value('json_data', 'apparent_power') }} AS apparent_power,
        {{ extract_grid_value('json_data', 'power_reactive') }} AS power_reactive,
        {{ extract_grid_value('json_data', 'current_phase_a') }} AS current_phase_a,
        {{ extract_grid_value('json_data', 'current_phase_b') }} AS current_phase_b,
        {{ extract_grid_value('json_data', 'current_phase_c') }} AS current_phase_c

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC
)

SELECT * FROM extracted_analogic
