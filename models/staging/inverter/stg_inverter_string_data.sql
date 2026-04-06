{{
    config(
        materialized='view'
    )
}}

/*
    ============================================================================
    STAGING MODEL: stg_inverter_string_data
    ============================================================================

    Descrição:
        Retorna o ÚLTIMO registro de cada string de cada inversor, incluindo
        informações do inversor e da usina.

    Arquitetura:
        Camada: STAGING (Silver Layer)
        Fonte: raw_inverter
        Destino: stg_inverter_string_data (View)

    Características:
        - View simples (sem materialização)
        - Retorna apenas o último registro de cada string por inversor
        - Inclui device_id do inversor e power_plant_id da usina

    Formato de Saída:
        timestamp | power_plant_id | device_id | device_name | string_number | string_current | state_simplified

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-01-19
    ============================================================================
*/

WITH latest_records AS (
    SELECT DISTINCT ON (device_id)
        timestamp,
        power_plant_id,
        device_id,
        json_data,
        (json_data->>'state_simplified')::NUMERIC::INTEGER AS state_simplified
    FROM {{ source('raw', 'raw_inverter') }}
    WHERE json_data->>'timestamp' IS NOT NULL
      AND timestamp >= '2026-01-06'::TIMESTAMP
    ORDER BY device_id, timestamp DESC
),

-- Gera números de 1 a 32 e extrai diretamente cada string_N_current
string_currents AS (
    SELECT
        ((lr.json_data->>'timestamp')::TIMESTAMPTZ - INTERVAL '3 hours')::TIMESTAMP AS timestamp,
        lr.power_plant_id,
        lr.device_id,
        lr.state_simplified,
        s.string_number,
        ROUND((lr.json_data->>(CONCAT('string_', s.string_number, '_current')))::NUMERIC, 2) AS string_current
    FROM latest_records lr
    CROSS JOIN LATERAL (
        SELECT UNNEST(ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]) AS string_number
    ) s
)

SELECT
    sc.timestamp,
    sc.power_plant_id,
    sc.device_id,
    d.device_name,
    sc.string_number,
    sc.string_current,
    sc.state_simplified
FROM string_currents sc
LEFT JOIN {{ source('reference', 'tb_devices') }} d
    ON d.id = sc.device_id
WHERE sc.string_current IS NOT NULL
