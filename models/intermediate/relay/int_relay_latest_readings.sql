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
    INTERMEDIATE MODEL: int_relay_latest_readings
    ============================================================================

    Descrição:
        Retorna o último valor registrado de cada campo analógico
        dos relés de proteção por power_plant_id.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_relay_analogic_data
    Atualização: Manual (ou via scheduled job)

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

WITH latest_per_device AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
    FROM {{ ref('stg_relay_analogic_data') }}
),

latest_data AS (
    SELECT
        device_id,
        power_plant_id,
        timestamp,
        active_power,
        line_voltage_ab,
        line_voltage_bc,
        line_voltage_ca,
        apparent_power,
        power_reactive,
        current_phase_a,
        current_phase_b,
        current_phase_c
    FROM latest_per_device
    WHERE rn = 1
)

SELECT * FROM latest_data
