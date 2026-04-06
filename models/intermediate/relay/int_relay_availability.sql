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
    INTERMEDIATE MODEL: int_relay_availability
    ============================================================================

    Descrição:
        Calcula a disponibilidade dos relés de proteção por usina.

        Disponibilidade = (Relés Ativos / Total de Relés) * 100

        Critérios para relé ATIVO:
        - breaker_position = 1 (disjuntor desatuado/fechado = normal) E
        - communication_failure = 1 (falha desatuada = sem falha) E
        - Leitura dos últimos 15 minutos

        Critérios para relé INATIVO:
        - breaker_position = 0 (disjuntor atuado/aberto = alarme) OU
        - communication_failure = 0 (falha atuada = com falha)
        - Leitura dos últimos 15 minutos

        Critérios para relé SEM DADOS:
        - Última leitura há mais de 15 minutos (dados desatualizados)
        - Sem leituras no banco (nunca comunicou)

    Camada: INTERMEDIATE (Silver Layer)
    Fontes:
        - tb_devices (total de relés cadastrados)
        - stg_relay_discrete_data (status discreto dos relés)

    Casos de Uso:
        - Input para mart_power_plants_overview
        - Monitoramento de disponibilidade de relés

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-27
    ============================================================================
*/

WITH total_relays_per_plant AS (
    SELECT
        d.power_plant_id,
        COUNT(*) AS total_relays
    FROM {{ source('reference', 'tb_devices') }} d
    INNER JOIN {{ source('reference', 'tb_power_plants') }} pp
        ON d.power_plant_id = pp.id
    WHERE d.device_type_id = 2
      AND d.is_active = TRUE
      AND pp.is_active = TRUE  -- updated at 2026/01/06 to filter only true
    GROUP BY d.power_plant_id
),

latest_relay_readings AS (
    SELECT DISTINCT ON (device_id)
        power_plant_id,
        device_id,
        timestamp,
        discrete_data_json
    FROM {{ ref('stg_relay_discrete_data') }}
    WHERE timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
    ORDER BY device_id, timestamp DESC
),

relay_status AS (
    SELECT
        power_plant_id,
        device_id,
        timestamp,
        -- Relé é considerado ATIVO se:
        -- 1. breaker_position = 1 (disjuntor desatuado/fechado = normal)
        -- 2. communication_failure = 1 (sem falha de comunicação = normal)
        -- 3. Leitura é recente (últimos 15 minutos)
        CASE
            WHEN (discrete_data_json->>'breaker_position')::INTEGER = 1
                 AND (discrete_data_json->>'communication_failure')::INTEGER = 1
                 AND timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_active,
        -- Flag para identificar relés sem dados recentes
        CASE
            WHEN timestamp <= (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_stale
    FROM latest_relay_readings
),

active_relays_per_plant AS (
    SELECT
        power_plant_id,
        COUNT(*) FILTER (WHERE is_active = TRUE) AS active_relays,
        COUNT(*) FILTER (WHERE is_active = FALSE AND is_stale = FALSE) AS inactive_relays,
        COUNT(*) FILTER (WHERE is_stale = TRUE) AS stale_relays
    FROM relay_status
    GROUP BY power_plant_id
),

availability_calc AS (
    SELECT
        t.power_plant_id,
        t.total_relays,
        COALESCE(a.active_relays, 0) AS active_relays,
        COALESCE(a.inactive_relays, 0) AS inactive_relays,
        -- Relés sem leitura recente (dados desatualizados > 15 min)
        COALESCE(a.stale_relays, 0) + 
        (t.total_relays - COALESCE(a.active_relays, 0) - COALESCE(a.inactive_relays, 0) - COALESCE(a.stale_relays, 0)) AS no_data_relays,
        -- Disponibilidade = relés ativos / total de relés
        ROUND(
            (COALESCE(a.active_relays, 0)::NUMERIC / NULLIF(t.total_relays, 0) * 100),
            2
        ) AS availability_pct
    FROM total_relays_per_plant t
    LEFT JOIN active_relays_per_plant a ON t.power_plant_id = a.power_plant_id
)

SELECT * FROM availability_calc
