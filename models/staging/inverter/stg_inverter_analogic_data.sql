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
    STAGING MODEL: stg_inverter_analogic
    ============================================================================

    Descrição:
        Extrai e normaliza dados analógicos dos inversores a partir da tabela
        raw_inverter. Converte valores do formato Grid Co (valor@timestamp)
        para valores numéricos puros.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_inverter
        Destino: stg_inverter_analogic (Hypertable TimescaleDB)
        Atualização: Incremental (a cada 5 minutos)

    Características:
        - Hypertable com chunks de 1 dia
        - Compressão automática após 7 dias
        - Retenção de 1 ano
        - Unique key: (device_id, timestamp)

    Autores: Heitor Teixeira
    Empresa: Axionics
    Data: 2025-11-30
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

extracted_analogic AS (
    SELECT DISTINCT ON (device_id, ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP)
        -- ====================================================================
        -- IDENTIFICADORES E TIMESTAMP
        -- ====================================================================
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,
        NULLIF(TRIM(json_data->>'serial_number'), '') AS serial_number,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - POTÊNCIA E ENERGIA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'active_power') }} AS active_power,
        {{ extract_grid_value('json_data', 'power_reactive') }} AS power_reactive,
        {{ extract_grid_value('json_data', 'power_input') }} AS power_input,
        {{ extract_grid_value('json_data', 'power_factor') }} AS power_factor,
        ROUND({{ extract_grid_value('json_data', 'daily_active_energy') }}::NUMERIC, 2) AS daily_active_energy,
        ROUND({{ extract_grid_value('json_data', 'cumulative_active_energy') }}::NUMERIC, 2) AS cumulative_active_energy,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - ELÉTRICA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'frequency') }} AS frequency,
        {{ extract_grid_value('json_data', 'efficiency') }} AS efficiency,

        -- Correntes de Fase
        {{ extract_grid_value('json_data', 'current_phase_a') }} AS current_phase_a,
        {{ extract_grid_value('json_data', 'current_phase_b') }} AS current_phase_b,
        {{ extract_grid_value('json_data', 'current_phase_c') }} AS current_phase_c,

        -- Tensões de Linha
        {{ extract_grid_value('json_data', 'line_voltage_ab') }} AS line_voltage_ab,
        {{ extract_grid_value('json_data', 'line_voltage_bc') }} AS line_voltage_bc,
        {{ extract_grid_value('json_data', 'line_voltage_ca') }} AS line_voltage_ca,

        -- Tensão das Strings
        {{ extract_grid_value('json_data', 'string_voltage') }} AS string_voltage,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - TEMPERATURA E ISOLAMENTO
        -- ====================================================================
        ROUND({{ extract_grid_value('json_data', 'temperature_internal') }}::NUMERIC, 2) AS temperature_internal,
        ROUND({{ extract_grid_value('json_data', 'resistance_insulation') }}::NUMERIC, 2) AS resistance_insulation,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - ESTADO DE OPERAÇÃO
        -- ====================================================================
        {{ extract_grid_value('json_data', 'state_operation', 'INTEGER') }} AS state_operation,
        {{ extract_grid_value('json_data', 'state_simplified', 'INTEGER') }} AS state_simplified

    FROM source_data
    ORDER BY
        device_id,
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP,
        raw_timestamp DESC
)

SELECT * FROM extracted_analogic
