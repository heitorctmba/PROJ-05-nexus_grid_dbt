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
    INTERMEDIATE MODEL: int_inverter_availability
    ============================================================================

    Descrição:
        Calcula a disponibilidade dos inversores por usina.

        Disponibilidade = (Inversores Ativos / Total de Inversores) * 100

        Critérios para inversor ATIVO:
        - state_simplified = 2 (produzindo)
        - Leitura dos últimos 15 minutos

        Critérios para inversor INATIVO:
        - state_simplified IN (0, 1, 3, 4)
        - Leitura dos últimos 15 minutos

        Critérios para inversor SEM DADOS:
        - Última leitura há mais de 15 minutos (dados desatualizados)
        - Sem leituras no banco (nunca comunicou)

        Valores de state_simplified:
        - 0, 1, 3, 4 = indisponível
        - 2 = disponível/produzindo

    Camada: INTERMEDIATE (Silver Layer)
    Fontes:
        - tb_devices (total de inversores cadastrados)
        - stg_inverter_analogic_data (status dos inversores)

    Casos de Uso:
        - Input para mart_power_plants_overview
        - Monitoramento de disponibilidade de inversores
        - Alertas de inversores offline

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-27
    ============================================================================
*/

WITH total_inverters_per_plant AS (
    SELECT
        d.power_plant_id,
        COUNT(*) AS total_inverters
    FROM {{ source('reference', 'tb_devices') }} d
    INNER JOIN {{ source('reference', 'tb_power_plants') }} pp
        ON d.power_plant_id = pp.id
    WHERE d.device_type_id = 1
      AND d.is_active = TRUE
      AND pp.is_active = TRUE  -- updated at 2026/01/06 to filter only true
    GROUP BY d.power_plant_id
),

-- OTIMIZAÇÃO: DISTINCT ON é mais eficiente que ROW_NUMBER no PostgreSQL
latest_only AS (
    SELECT DISTINCT ON (device_id)
        power_plant_id,
        device_id,
        timestamp,
        state_simplified
    FROM {{ ref('stg_inverter_analogic_data') }}
    WHERE timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
    ORDER BY device_id, timestamp DESC
),

inverter_status AS (
    SELECT
        power_plant_id,
        device_id,
        timestamp,
        -- Inversor é considerado ATIVO apenas se:
        -- 1. state_simplified = 2 (produzindo)
        -- 2. Leitura é recente (últimos 15 minutos)
        CASE
            WHEN state_simplified = 2 
             AND timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_active,
        -- Flag para identificar inversores sem dados recentes
        CASE
            WHEN timestamp <= (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '15 minutes'
            THEN TRUE
            ELSE FALSE
        END AS is_stale
    FROM latest_only
),

active_inverters_per_plant AS (
    SELECT
        power_plant_id,
        COUNT(*) FILTER (WHERE is_active = TRUE) AS active_inverters,
        COUNT(*) FILTER (WHERE is_active = FALSE AND is_stale = FALSE) AS inactive_inverters,
        COUNT(*) FILTER (WHERE is_stale = TRUE) AS stale_inverters
    FROM inverter_status
    GROUP BY power_plant_id
),

availability_calc AS (
    SELECT
        t.power_plant_id,
        t.total_inverters,
        COALESCE(a.active_inverters, 0) AS active_inverters,
        COALESCE(a.inactive_inverters, 0) AS inactive_inverters,
        -- Inversores sem leitura recente (dados desatualizados > 15 min)
        COALESCE(a.stale_inverters, 0) + 
        (t.total_inverters - COALESCE(a.active_inverters, 0) - COALESCE(a.inactive_inverters, 0) - COALESCE(a.stale_inverters, 0)) AS no_data_inverters,
        -- Disponibilidade = inversores ativos / total de inversores
        ROUND(
            (COALESCE(a.active_inverters, 0)::NUMERIC / NULLIF(t.total_inverters, 0) * 100),
            2
        ) AS availability_pct
    FROM total_inverters_per_plant t
    LEFT JOIN active_inverters_per_plant a ON t.power_plant_id = a.power_plant_id
)

SELECT * FROM availability_calc
