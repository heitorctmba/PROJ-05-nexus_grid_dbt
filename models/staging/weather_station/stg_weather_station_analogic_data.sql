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
    STAGING MODEL: stg_weather_station_analogic_data
    ============================================================================

    Descrição:
        Extrai e normaliza dados analógicos das estações meteorológicas
        a partir da tabela raw_weather_station. Converte valores do formato
        Grid Co (valor@timestamp) para valores numéricos puros.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_weather_station
        Destino: stg_weather_station_analogic_data (Hypertable TimescaleDB)
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
    FROM {{ source('raw', 'raw_weather_station') }}
    WHERE json_data->>'timestamp' IS NOT NULL
      AND timestamp >= NOW() - INTERVAL '2 months'

    {% if is_incremental() %}
        -- Processar apenas dados novos (incrementais)
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

extracted_analogic AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        -- ====================================================================
        -- IDENTIFICADORES E TIMESTAMP
        -- ====================================================================
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - IRRADIÂNCIA
        -- ====================================================================
        ROUND({{ extract_grid_value('json_data', 'irradiance_ghi') }}::NUMERIC, 2) AS irradiance_ghi,
        ROUND({{ extract_grid_value('json_data', 'irradiance_poa') }}::NUMERIC, 2) AS irradiance_poa,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - TEMPERATURA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'air_temperature') }} AS air_temperature,
        ROUND({{ extract_grid_value('json_data', 'module_temperature') }}::NUMERIC, 2) AS module_temperature,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - CHUVA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'rain_signal') }} AS rain_signal,
        ROUND({{ extract_grid_value('json_data', 'accumulated_rain') }}::NUMERIC, 2) AS accumulated_rain,
        ROUND({{ extract_grid_value('json_data', 'hourly_accumulated_rain') }}::NUMERIC, 2) AS hourly_accumulated_rain,
        {{ extract_grid_value('json_data', 'monthly_accumulated_rain') }} AS monthly_accumulated_rain

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC  -- Em caso de duplicatas, mantém o registro mais recente da raw
)

SELECT * FROM extracted_analogic
