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
    STAGING MODEL: stg_tracker_discrete_data
    ============================================================================

    Descrição:
        Extrai dados discretos (estados e alarmes) dos trackers solares em
        formato JSON flexível. Separa campos discretos dos analógicos.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_tracker
        Destino: stg_tracker_discrete_data (Hypertable TimescaleDB)
        Atualização: Incremental (a cada 5 minutos)

    Estratégia de Dados:
        - Armazena campos discretos em formato JSONB
        - Exclui campos analógicos (já mapeados em stg_tracker_analogic_data)
        - Parsing e join com catálogo será feito em camada INTERMEDIATE

    Campos Discretos dos Trackers:
        Estados: auto_on, manu_on, sleep_on, trk_curso
        Alarmes: bat_crit, bat_ins_al, bat_ins_seg, bat_min,
                 bot_emerg, flh_com, flh_leve, flh_zigbee, out_of_range

    Características:
        - Hypertable com chunks de 1 dia
        - Compressão automática após 7 dias
        - Retenção de 1 ano
        - Unique key: (device_id, timestamp)

    Referência de campos: TRACKER_FIELD_REFERENCE.md

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2026-02-26
    ============================================================================
*/

WITH source_data AS (
    SELECT
        timestamp AS raw_timestamp,
        power_plant_id,
        device_id,
        json_data
    FROM {{ source('raw', 'raw_tracker') }}
    WHERE json_data->>'timestamp' IS NOT NULL

    {% if is_incremental() %}
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

-- Lista de campos analógicos que devem ser excluídos
excluded_fields AS (
    SELECT unnest(ARRAY[
        'timestamp',
        'posat',
        'posal'
    ]) AS field_name
),

-- Extrair apenas campos discretos (estados/alarmes)
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
