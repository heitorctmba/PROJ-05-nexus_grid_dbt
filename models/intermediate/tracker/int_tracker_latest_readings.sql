{{
    config(
        materialized='table',
        on_schema_change='fail',
        indexes=[
            {'columns': ['device_id'], 'type': 'btree'},
            {'columns': ['power_plant_id'], 'type': 'btree'}
        ]
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_tracker_latest_readings
    ============================================================================

    Descrição:
        Retorna o último valor registrado de cada campo analógico
        dos trackers solares por power_plant_id.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_tracker_analogic_data
    Atualização: Manual (ou via scheduled job)

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2026-02-26
    ============================================================================
*/

WITH latest_per_device AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) AS rn
    FROM {{ ref('stg_tracker_analogic_data') }}
),

latest_data AS (
    SELECT
        device_id,
        power_plant_id,
        timestamp,
        posat,
        posal
    FROM latest_per_device
    WHERE rn = 1
)

SELECT * FROM latest_data
