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
    INTERMEDIATE MODEL: int_weather_station_latest_readings
    ============================================================================

    Descrição:
        Retorna o último valor registrado de cada campo analógico
        das estações meteorológicas por power_plant_id.

        Como geralmente há apenas 1 estação meteorológica por usina,
        este modelo agrega os dados por usina.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_weather_station_analogic_data
    Atualização: Manual (ou via scheduled job)

    Casos de Uso:
        - Input para mart_power_plants_overview (irradiance da usina)
        - Dashboards de monitoramento climático

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

WITH latest_per_device AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
    FROM {{ ref('stg_weather_station_analogic_data') }}
),

latest_data AS (
    SELECT
        device_id,
        power_plant_id,
        timestamp,
        irradiance_ghi,
        irradiance_poa,
        air_temperature,
        module_temperature,
        rain_signal,
        accumulated_rain,
        hourly_accumulated_rain,
        monthly_accumulated_rain
    FROM latest_per_device
    WHERE rn = 1
)

SELECT * FROM latest_data
