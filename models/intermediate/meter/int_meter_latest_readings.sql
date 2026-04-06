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
    INTERMEDIATE MODEL: int_meter_latest_readings
    ============================================================================

    Descrição:
        Retorna o último valor registrado de cada campo analógico
        dos medidores de faturamento por power_plant_id.

        Como geralmente há apenas 1 medidor por usina, este modelo
        agrega os dados por usina.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_meter_analogic_data
    Atualização: Manual (ou via scheduled job)

    Casos de Uso:
        - Input para mart_power_plants_overview (active_power da usina)
        - Dashboards de monitoramento de medidores

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

WITH latest_per_device AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
    FROM {{ ref('stg_meter_analogic_data') }}
),

latest_data AS (
    SELECT
        device_id,
        power_plant_id,
        timestamp,
        active_power,
        power_reactive,
        apparent_power,
        power_factor,
        exported_active_energy,
        imported_active_energy,
        exported_reactive_energy,
        imported_reactive_energy,
        frequency,
        current_phase_a,
        current_phase_b,
        current_phase_c,
        line_voltage_ab,
        line_voltage_bc,
        line_voltage_ca
    FROM latest_per_device
    WHERE rn = 1
)

SELECT * FROM latest_data
