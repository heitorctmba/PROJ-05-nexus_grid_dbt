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
    STAGING MODEL (TESTE): stg_inverter_analogic_data_teste
    ============================================================================

    Modelo de teste para validação do fix de duplicação.

    PROBLEMA identificado no modelo original:
        O filtro incremental compara raw.timestamp (TIMESTAMPTZ, UTC) com
        MAX(stg.timestamp) (TIMESTAMP WITHOUT TZ, armazenado em horário BRT).
        O PostgreSQL trata o valor sem fuso como UTC na comparação, criando
        um desvio de 3 horas. Isso faz com que cada execução reprocesse as
        últimas 3 horas de dados já inseridos, gerando 37 duplicatas por leitura
        ao longo do tempo (3h ÷ 5min = 36 re-appends + 1 insert original).

    CORREÇÃO aplicada:
        AND timestamp > (SELECT MAX(timestamp) AT TIME ZONE 'America/Sao_Paulo' FROM {{ this }})

        O AT TIME ZONE converte o timestamp BRT (sem fuso) para TIMESTAMPTZ
        antes de comparar com raw.timestamp (TIMESTAMPTZ UTC) — eliminando
        o desvio de 3 horas.

    Comparar com: dbt.stg_inverter_analogic_data (modelo original)

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2026-03-10
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
      -- TEMPORÁRIO: limitar ao mês atual para agilizar full refresh
      AND timestamp >= date_trunc('month', NOW())

    {% if is_incremental() %}
        -- FIX: AT TIME ZONE converte BRT (sem TZ) para TIMESTAMPTZ antes de comparar
        -- com raw.timestamp (TIMESTAMPTZ UTC), eliminando o desvio de 3h que causava
        -- 37 duplicatas por leitura no modelo original.
        AND timestamp > (SELECT MAX(timestamp) AT TIME ZONE 'America/Sao_Paulo' FROM {{ this }})
    {% endif %}
),

extracted_analogic AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,
        NULLIF(TRIM(json_data->>'serial_number'), '') AS serial_number,

        {{ extract_grid_value('json_data', 'active_power') }} AS active_power,
        {{ extract_grid_value('json_data', 'power_reactive') }} AS power_reactive,
        {{ extract_grid_value('json_data', 'power_input') }} AS power_input,
        {{ extract_grid_value('json_data', 'power_factor') }} AS power_factor,
        ROUND({{ extract_grid_value('json_data', 'daily_active_energy') }}::NUMERIC, 2) AS daily_active_energy,
        ROUND({{ extract_grid_value('json_data', 'cumulative_active_energy') }}::NUMERIC, 2) AS cumulative_active_energy,

        {{ extract_grid_value('json_data', 'frequency') }} AS frequency,
        {{ extract_grid_value('json_data', 'efficiency') }} AS efficiency,

        {{ extract_grid_value('json_data', 'current_phase_a') }} AS current_phase_a,
        {{ extract_grid_value('json_data', 'current_phase_b') }} AS current_phase_b,
        {{ extract_grid_value('json_data', 'current_phase_c') }} AS current_phase_c,

        {{ extract_grid_value('json_data', 'line_voltage_ab') }} AS line_voltage_ab,
        {{ extract_grid_value('json_data', 'line_voltage_bc') }} AS line_voltage_bc,
        {{ extract_grid_value('json_data', 'line_voltage_ca') }} AS line_voltage_ca,

        {{ extract_grid_value('json_data', 'string_voltage') }} AS string_voltage,

        ROUND({{ extract_grid_value('json_data', 'temperature_internal') }}::NUMERIC, 2) AS temperature_internal,
        ROUND({{ extract_grid_value('json_data', 'resistance_insulation') }}::NUMERIC, 2) AS resistance_insulation,

        {{ extract_grid_value('json_data', 'state_operation', 'INTEGER') }} AS state_operation,
        {{ extract_grid_value('json_data', 'state_simplified', 'INTEGER') }} AS state_simplified

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC
)

SELECT * FROM extracted_analogic
