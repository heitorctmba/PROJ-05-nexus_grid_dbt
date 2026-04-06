{{
    config(
        materialized='table',
        on_schema_change='fail',
        indexes=[
            {'columns': ['power_plant_id'], 'type': 'btree'},
            {'columns': ['power_plant_name'], 'type': 'btree'}
        ]
    )
}}

/*
    ============================================================================
    MART MODEL: mart_power_plants_overview
    ============================================================================

    Descrição:
        Visão geral consolidada das usinas solares para dashboards executivos.
        Combina dados de cadastro (tb_power_plants) com métricas operacionais
        em tempo real dos medidores e loggers.

    Camada: MARTS (Gold Layer)
    Fontes:
        - tb_power_plants (cadastro)
        - int_meter_latest_readings (active_power da usina)
        - int_logger_latest_readings (daily_active_energy da usina)
        - int_weather_station_latest_readings (irradiance_poa da usina)
        - int_inverter_availability (disponibilidade dos inversores)
        - int_relay_availability (disponibilidade dos relés)

    Casos de Uso:
        - Dashboard geral de usinas no Grafana
        - Visão executiva de performance
        - Monitoramento em tempo real

    Campos Calculados (A IMPLEMENTAR):
        - pr (Performance Ratio): A definir

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

WITH power_plants_base AS (
    SELECT
        id AS power_plant_id,
        name AS power_plant_name,
        capacity AS rated_power_kw,
        location,
        is_active
    FROM {{ source('reference', 'tb_power_plants') }}
    WHERE is_active = TRUE
),

-- NOTA: Desabilitado temporariamente - aguardando dados de meter em produção
-- meter_data AS (
--     SELECT
--         power_plant_id,
--         active_power,
--         exported_active_energy,
--         timestamp AS meter_last_reading_timestamp
--     FROM {{ ref('int_meter_latest_readings') }}
-- ),

-- NOTA: Desabilitado temporariamente - aguardando dados de logger em produção
-- logger_data AS (
--     SELECT
--         power_plant_id,
--         daily_active_energy,
--         timestamp AS logger_last_reading_timestamp
--     FROM {{ ref('int_logger_latest_readings') }}
-- ),

weather_data AS (
    SELECT
        power_plant_id,
        irradiance_poa,
        timestamp AS weather_last_reading_timestamp
    FROM {{ ref('int_weather_station_latest_readings') }}
),

inverter_availability_data AS (
    SELECT
        power_plant_id,
        availability_pct
    FROM {{ ref('int_inverter_availability') }}
),

relay_availability_data AS (
    SELECT
        power_plant_id,
        availability_pct
    FROM {{ ref('int_relay_availability') }}
),
alarm_status AS (
    WITH latest_states AS (
        SELECT DISTINCT ON (power_plant_id, device_id, code)
            power_plant_id,
            device_id,
            code,
            type,
            severity,
            acknowledged
        FROM {{ ref('int_events_alarms') }}
        ORDER BY power_plant_id, device_id, code, timestamp DESC
    ),
    high_alarms AS (
        SELECT
            power_plant_id,
            acknowledged
        FROM latest_states
        WHERE type = 'alarm'
          AND severity = 'high'
    )
    SELECT
        power_plant_id,
        -- Se tem pelo menos 1 alarme high não reconhecido
        BOOL_OR(acknowledged = FALSE) AS has_unacknowledged_alarm,
        -- Se tem algum alarme high (para diferenciar acknowledged de generating)
        TRUE AS has_high_alarm
    FROM high_alarms
    GROUP BY power_plant_id
),

recent_communication AS (
    SELECT
        power_plant_id,
        MAX(timestamp) AS last_communication
    FROM {{ ref('int_inverter_latest_readings') }}
    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '3 hours') - INTERVAL '30 minutes'
    GROUP BY power_plant_id
),

power_plant_status_data AS (
    SELECT
        pp.power_plant_id,
        CASE
            WHEN rc.last_communication IS NULL THEN 'no_communication'
            WHEN als.has_unacknowledged_alarm = TRUE THEN 'critical'
            WHEN als.has_high_alarm = TRUE THEN 'acknowledged'
            ELSE 'generating'
        END AS power_plant_status
    FROM power_plants_base pp
    LEFT JOIN alarm_status als ON pp.power_plant_id = als.power_plant_id
    LEFT JOIN recent_communication rc ON pp.power_plant_id = rc.power_plant_id
),
-- ====================================================================
-- ACTIVE POWER (calculado com base nas medidas do inversor)
-- de maneira mais geral da pra ser assim, a soma de todos os inversores
-- de cada uma das usinas.
-- ====================================================================
active_power_data AS (
    SELECT
        power_plant_id,
        SUM(value_numeric)::numeric AS active_power_kw
    FROM {{ ref('int_inverter_latest_readings') }}
    WHERE field_name = 'Active Power'
      AND value_numeric IS NOT NULL
    GROUP BY power_plant_id
),

-- ====================================================================
-- CÁLCULO DO PR (PERFORMANCE RATIO)
-- ====================================================================
-- ET: Soma do daily_active_energy no último timestamp de cada inversor
-- OTIMIZAÇÃO: DISTINCT ON é mais eficiente que ROW_NUMBER no PostgreSQL
inverter_daily_energy AS (
    SELECT
        power_plant_id,
        SUM(daily_active_energy) AS et_kwh
    FROM (
        SELECT DISTINCT ON (device_id)
            power_plant_id,
            device_id,
            daily_active_energy
        FROM {{ ref('stg_inverter_analogic_data') }}
        WHERE daily_active_energy IS NOT NULL
          AND timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
        ORDER BY device_id, timestamp DESC
    ) latest_per_inverter
    GROUP BY power_plant_id
),

-- HT: Soma de todos os valores de irradiance_poa do dia dividido por 12
-- Usa a data de HOJE para buscar os dados de irradiância
daily_irradiation AS (
    SELECT
        ws.power_plant_id,
        SUM(ws.irradiance_poa) / 12.0 AS ht_kwh_m2
    FROM {{ ref('stg_weather_station_analogic_data') }} ws
    WHERE DATE(ws.timestamp) = (CURRENT_DATE AT TIME ZONE 'America/Sao_Paulo')::DATE
      AND ws.irradiance_poa IS NOT NULL
      AND ws.device_id <> 5
    GROUP BY ws.power_plant_id
),

combined AS (
    SELECT
        -- ====================================================================
        -- IDENTIFICAÇÃO DA USINA
        -- ====================================================================
        pp.power_plant_id,
        pp.power_plant_name,

        -- ====================================================================
        -- CAPACIDADE NOMINAL
        -- ====================================================================
        pp.rated_power_kw,

        -- ====================================================================
        -- POTÊNCIA ATIVA ATUAL (do medidor)
        -- NOTA: Temporariamente NULL - aguardando dados de meter em produção
        -- ====================================================================
        -- NULL::NUMERIC AS active_power_kw,
        CASE
            WHEN COALESCE(ps.power_plant_status, 'no_communication') = 'no_communication'
                THEN 0
            ELSE COALESCE(ap.active_power_kw, 0)
        END AS active_power_kw,

        -- ====================================================================
        -- ENERGIA DIÁRIA (acumulado do dia atual via inversores)
        -- ====================================================================
        ROUND(COALESCE(et.et_kwh, 0)::NUMERIC, 2) AS daily_energy_kwh,

        -- ====================================================================
        -- IRRADIÂNCIA (da estação meteorológica)
        -- ====================================================================
        ROUND(COALESCE(w.irradiance_poa, 0)::NUMERIC, 2) AS irradiance_wm2,

        -- ====================================================================
        -- DISPONIBILIDADE DOS INVERSORES
        -- ====================================================================
        COALESCE(ia.availability_pct, 0) AS inverter_availability_pct,

        -- ====================================================================
        -- DISPONIBILIDADE DOS RELÉS
        -- ====================================================================
        COALESCE(ra.availability_pct, 0) AS rates_availability_pct,

        -- ====================================================================
        -- STATUS OPERACIONAL DA USINA
        -- ====================================================================
        COALESCE(ps.power_plant_status, 'no_communication') AS power_plant_status,

        -- ====================================================================
        -- PERFORMANCE RATIO (PR)
        -- Fórmula: PR = (ET / PO) * (G / HT)
        -- ET = Energia gerada no dia (kWh)
        -- PO = Potência nominal da usina (kWp)
        -- G = Irradiância de referência (1000 W/m²)
        -- HT = Irradiação do dia (kWh/m²)
        -- ====================================================================
        CASE
            WHEN pp.rated_power_kw > 0
                 AND COALESCE(ht.ht_kwh_m2, 0) > 0
            THEN ROUND(
                ((COALESCE(et.et_kwh, 0) / pp.rated_power_kw) * (1000.0 / ht.ht_kwh_m2) * 100.0)::numeric,
                2
            )
            ELSE NULL
        END AS pr_pct,

        -- ====================================================================
        -- METADADOS
        -- ====================================================================
        pp.location,
        NULL::TIMESTAMP AS meter_last_reading_timestamp,
        NULL::TIMESTAMP AS logger_last_reading_timestamp,
        w.weather_last_reading_timestamp,
        CURRENT_TIMESTAMP AS updated_at

    FROM power_plants_base pp
    -- LEFT JOIN meter_data m ON pp.power_plant_id = m.power_plant_id
    -- LEFT JOIN logger_data l ON pp.power_plant_id = l.power_plant_id
    LEFT JOIN weather_data w ON pp.power_plant_id = w.power_plant_id
    LEFT JOIN inverter_availability_data ia ON pp.power_plant_id = ia.power_plant_id
    LEFT JOIN relay_availability_data ra ON pp.power_plant_id = ra.power_plant_id
    LEFT JOIN power_plant_status_data ps ON pp.power_plant_id = ps.power_plant_id
    LEFT JOIN active_power_data ap ON pp.power_plant_id = ap.power_plant_id
    LEFT JOIN inverter_daily_energy et ON pp.power_plant_id = et.power_plant_id
    LEFT JOIN daily_irradiation ht ON pp.power_plant_id = ht.power_plant_id
)

SELECT * FROM combined
ORDER BY power_plant_name
