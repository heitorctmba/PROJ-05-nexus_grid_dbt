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
    STAGING MODEL (TESTE): stg_weather_station_analogic_data_teste
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

    Comparar com: dbt.stg_weather_station_analogic_data (modelo original)

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
    FROM {{ source('raw', 'raw_weather_station') }}
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

        ROUND({{ extract_grid_value('json_data', 'irradiance_ghi') }}::NUMERIC, 2) AS irradiance_ghi,
        ROUND({{ extract_grid_value('json_data', 'irradiance_poa') }}::NUMERIC, 2) AS irradiance_poa,

        {{ extract_grid_value('json_data', 'air_temperature') }} AS air_temperature,
        ROUND({{ extract_grid_value('json_data', 'module_temperature') }}::NUMERIC, 2) AS module_temperature,

        {{ extract_grid_value('json_data', 'rain_signal') }} AS rain_signal,
        ROUND({{ extract_grid_value('json_data', 'accumulated_rain') }}::NUMERIC, 2) AS accumulated_rain,
        ROUND({{ extract_grid_value('json_data', 'hourly_accumulated_rain') }}::NUMERIC, 2) AS hourly_accumulated_rain,
        {{ extract_grid_value('json_data', 'monthly_accumulated_rain') }} AS monthly_accumulated_rain

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC
)

SELECT * FROM extracted_analogic
