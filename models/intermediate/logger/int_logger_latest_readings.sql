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
    INTERMEDIATE MODEL: int_logger_latest_readings
    ============================================================================

    Descrição:
        Retorna o último valor registrado de cada campo analógico
        dos smart loggers por power_plant_id.

        Como geralmente há apenas 1 logger por usina, este modelo
        agrega os dados por usina.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_logger_analogic_data
    Atualização: Manual (ou via scheduled job)

    Casos de Uso:
        - Input para mart_power_plants_overview (daily_active_energy da usina)
        - Dashboards de monitoramento de loggers

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

WITH latest_data AS (
    SELECT DISTINCT ON (device_id)
        device_id,
        power_plant_id,
        timestamp,
        active_power,
        daily_active_energy,
        cumulative_active_energy,
        power_reactive,
        dc_input_power,
        active_power_limit,
        power_factor,
        efficiency,
        dc_current,
        current_phase_a,
        current_phase_b,
        current_phase_c,
        line_voltage_ab,
        line_voltage_bc,
        line_voltage_ca,
        inverters_producing_count,
        logger_state,
        operation_state,
        breaker_position
    FROM {{ ref('stg_logger_analogic_data') }}
    WHERE timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
    ORDER BY device_id, timestamp DESC
)

SELECT * FROM latest_data
