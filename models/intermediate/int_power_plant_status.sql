{{
    config(
        materialized='view',
        tags=['int']
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_power_plant_status
    ============================================================================

    Descrição:
        Calcula o status operacional de cada usina em tempo real baseado em:
        - Comunicação com devices (últimos 30 minutos)
        - Alarmes críticos ativos (severidade high)

    Camada: INTERMEDIATE (Gold Layer)
    Fontes:
        - stg_*_analogic_data (todas as fontes de dados)
        - int_events_alarms (alarmes críticos)

    Lógica de Status:
        - 'generating': Usina recebendo dados de devices nos últimos 30 min
                        E nenhum alarme de severidade high ativo
        - 'offline': Usina com algum device retornando alarme severidade high
        - 'no_communication': Usina NÃO recebeu dados de nenhum device nos últimos 30 min

    Prioridade de Status (do mais crítico para o menos):
        1. offline (alarme crítico)
        2. no_communication (sem dados)
        3. generating (operação normal)

    Casos de Uso:
        - Dashboard de status operacional
        - Alertas em tempo real
        - Monitoramento de saúde das usinas

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-22
    ============================================================================
*/

WITH all_devices_data AS (
    -- União de todos os devices para verificar comunicação
    -- Nota: Os timestamps já estão ajustados para UTC-3 nos modelos staging
    SELECT
        power_plant_id,
        timestamp,
        'inverter' AS device_type
    FROM {{ ref('stg_inverter_analogic_data') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'

    UNION ALL

    SELECT
        power_plant_id,
        timestamp,
        'logger' AS device_type
    FROM {{ ref('stg_logger_analogic_data') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'

    UNION ALL

    SELECT
        power_plant_id,
        timestamp,
        'meter' AS device_type
    FROM {{ ref('stg_meter_analogic_data') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'

    UNION ALL

    SELECT
        power_plant_id,
        timestamp,
        'relay' AS device_type
    FROM {{ ref('stg_relay_analogic_data') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'

    UNION ALL

    SELECT
        power_plant_id,
        timestamp,
        'weather_station' AS device_type
    FROM {{ ref('stg_weather_station_analogic_data') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'
),

-- Verificar se usinas têm dados recentes (últimos 30 minutos)
recent_communication AS (
    SELECT
        power_plant_id,
        MAX(timestamp) AS last_communication,
        COUNT(DISTINCT device_type) AS devices_communicating
    FROM all_devices_data
    GROUP BY power_plant_id
),

-- Alarmes críticos ativos (severidade high)
-- NOTA: Usa type='alarm' do catálogo em vez de value=1, pois alguns alarmes
-- como communication_failure têm lógica invertida (value=0 = alarme atuado)
critical_alarms AS (
    SELECT DISTINCT
        power_plant_id,
        1 AS has_critical_alarm
    FROM {{ ref('int_events_alarms') }}
    WHERE type = 'alarm'      -- Alarme atuado (baseado no catálogo)
      AND severity = 'high'
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'
),

-- Obter todas as usinas ativas
active_power_plants AS (
    SELECT
        id AS power_plant_id
    FROM {{ source('reference', 'tb_power_plants') }}
    WHERE is_active = TRUE
),

-- Calcular status de cada usina
power_plant_status_calc AS (
    SELECT
        pp.power_plant_id,
        COALESCE(rc.last_communication, NULL) AS last_communication_timestamp,
        COALESCE(rc.devices_communicating, 0) AS devices_communicating,
        COALESCE(ca.has_critical_alarm, 0) AS has_critical_alarm,

        -- Determinar status com base nas regras de negócio
        CASE
            -- Prioridade 1: Alarme crítico ativo
            WHEN ca.has_critical_alarm = 1 THEN 'offline'

            -- Prioridade 2: Sem comunicação (nenhum device enviou dados)
            WHEN rc.devices_communicating IS NULL OR rc.devices_communicating = 0 THEN 'no_communication'

            -- Prioridade 3: Operação normal (devices comunicando E sem alarmes críticos)
            ELSE 'generating'
        END AS power_plant_status

    FROM active_power_plants pp
    LEFT JOIN recent_communication rc ON pp.power_plant_id = rc.power_plant_id
    LEFT JOIN critical_alarms ca ON pp.power_plant_id = ca.power_plant_id
)

SELECT
    power_plant_id,
    power_plant_status,
    last_communication_timestamp,
    devices_communicating,
    CASE WHEN has_critical_alarm = 1 THEN TRUE ELSE FALSE END AS has_critical_alarm,
    CURRENT_TIMESTAMP AS calculated_at
FROM power_plant_status_calc
