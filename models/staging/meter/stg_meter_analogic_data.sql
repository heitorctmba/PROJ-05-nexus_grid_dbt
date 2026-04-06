{{
    config(
        materialized='incremental',
        unique_key=['device_id', 'timestamp'],
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
    STAGING MODEL: stg_meter_analogic_data
    ============================================================================

    Descrição:
        Extrai e normaliza dados analógicos dos medidores de faturamento
        a partir da tabela raw_meter. Converte valores do formato Grid Co
        (valor@timestamp) para valores numéricos puros.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_meter
        Destino: stg_meter_analogic_data (Hypertable TimescaleDB)
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
        timestamp,
        power_plant_id,
        device_id,
        json_data
    FROM {{ source('raw', 'raw_meter') }}
    WHERE json_data->>'timestamp' IS NOT NULL

    {% if is_incremental() %}
        -- Processar apenas dados novos (incrementais)
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

extracted_analogic AS (
    SELECT
        -- ====================================================================
        -- IDENTIFICADORES E TIMESTAMP
        -- ====================================================================
        ((json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        power_plant_id,
        device_id,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - POTÊNCIA E ENERGIA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'active_power') }} AS active_power,
        {{ extract_grid_value('json_data', 'power_reactive') }} AS power_reactive,
        {{ extract_grid_value('json_data', 'apparent_power') }} AS apparent_power,
        {{ extract_grid_value('json_data', 'power_factor') }} AS power_factor,

        {{ extract_grid_value('json_data', 'exported_active_energy') }} AS exported_active_energy,
        {{ extract_grid_value('json_data', 'imported_active_energy') }} AS imported_active_energy,
        {{ extract_grid_value('json_data', 'exported_reactive_energy') }} AS exported_reactive_energy,
        {{ extract_grid_value('json_data', 'imported_reactive_energy') }} AS imported_reactive_energy,

        -- ====================================================================
        -- CAMPOS ANALÓGICOS - ELÉTRICA
        -- ====================================================================
        {{ extract_grid_value('json_data', 'frequency') }} AS frequency,

        -- Correntes de Fase
        {{ extract_grid_value('json_data', 'current_phase_a') }} AS current_phase_a,
        {{ extract_grid_value('json_data', 'current_phase_b') }} AS current_phase_b,
        {{ extract_grid_value('json_data', 'current_phase_c') }} AS current_phase_c,

        -- Tensões de Linha
        {{ extract_grid_value('json_data', 'line_voltage_ab') }} AS line_voltage_ab,
        {{ extract_grid_value('json_data', 'line_voltage_bc') }} AS line_voltage_bc,
        {{ extract_grid_value('json_data', 'line_voltage_ca') }} AS line_voltage_ca

    FROM source_data
)

SELECT * FROM extracted_analogic
