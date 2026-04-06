{{
    config(
        materialized='table',
        on_schema_change='fail',
        indexes=[
            {'columns': ['power_plant_id'], 'type': 'btree'}
        ]
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_tracker_availability
    ============================================================================

    Descrição:
        Calcula a disponibilidade dos trackers solares por usina.

        Disponibilidade = (Trackers Ativos / Total de Trackers) * 100

        Critérios para tracker ATIVO:
        - flh_com = 0 (sem falha de comunicação) E
        - Leitura dos últimos 15 minutos

        Critérios para tracker INATIVO:
        - flh_com = 1 (falha de comunicação ativa) E
        - Leitura dos últimos 15 minutos

        Critérios para tracker SEM DADOS:
        - Última leitura há mais de 15 minutos (dados desatualizados)
        - Sem leituras no banco (nunca comunicou)

    Camada: INTERMEDIATE (Silver Layer)
    Fontes:
        - tb_devices (total de trackers cadastrados)
        - stg_tracker_discrete_data (status discreto dos trackers)

    Casos de Uso:
        - Input para mart_power_plants_overview
        - Monitoramento de disponibilidade de trackers

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2026-02-26
    ============================================================================
*/

WITH total_trackers_per_plant AS (
    SELECT
        d.power_plant_id,
        COUNT(*) AS total_trackers
    FROM {{ source('reference', 'tb_devices') }} d
    INNER JOIN {{ source('reference', 'tb_power_plants') }} pp
        ON d.power_plant_id = pp.id
    WHERE d.device_type_id = 4
      AND d.is_active = TRUE
      AND pp.is_active = TRUE
    GROUP BY d.power_plant_id
),

latest_tracker_readings AS (
    SELECT DISTINCT ON (device_id)
        power_plant_id,
        device_id,
        timestamp,
        discrete_data_json
    FROM {{ ref('stg_tracker_discrete_data') }}
    WHERE timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
    ORDER BY device_id, timestamp DESC
),

tracker_status AS (
    SELECT
        power_plant_id,
        device_id,
        timestamp,
        -- Tracker é considerado ATIVO se:
        -- 1. flh_com = 0 (sem falha de comunicação = normal)
        -- 2. Leitura é recente (últimos 15 minutos)
        CASE
            WHEN (discrete_data_json->>'flh_com')::INTEGER = 0
                 AND timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_active,
        -- Flag para identificar trackers sem dados recentes
        CASE
            WHEN timestamp <= (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_stale
    FROM latest_tracker_readings
),

active_trackers_per_plant AS (
    SELECT
        power_plant_id,
        COUNT(*) FILTER (WHERE is_active = TRUE) AS active_trackers,
        COUNT(*) FILTER (WHERE is_active = FALSE AND is_stale = FALSE) AS inactive_trackers,
        COUNT(*) FILTER (WHERE is_stale = TRUE) AS stale_trackers
    FROM tracker_status
    GROUP BY power_plant_id
),

availability_calc AS (
    SELECT
        t.power_plant_id,
        t.total_trackers,
        COALESCE(a.active_trackers, 0) AS active_trackers,
        COALESCE(a.inactive_trackers, 0) AS inactive_trackers,
        -- Trackers sem leitura recente (dados desatualizados > 15 min)
        COALESCE(a.stale_trackers, 0) +
        (t.total_trackers - COALESCE(a.active_trackers, 0) - COALESCE(a.inactive_trackers, 0) - COALESCE(a.stale_trackers, 0)) AS no_data_trackers,
        -- Disponibilidade = trackers ativos / total de trackers
        ROUND(
            (COALESCE(a.active_trackers, 0)::NUMERIC / NULLIF(t.total_trackers, 0) * 100),
            2
        ) AS availability_pct
    FROM total_trackers_per_plant t
    LEFT JOIN active_trackers_per_plant a ON t.power_plant_id = a.power_plant_id
)

SELECT * FROM availability_calc
