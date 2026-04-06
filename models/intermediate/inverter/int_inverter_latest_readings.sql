{{
    config(
        materialized='view'
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_inverter_latest_readings
    ============================================================================

    Descrição:
        Despivota dados analógicos dos inversores em formato tabular
        (campo, valor, unidade, timestamp) para facilitar visualização
        em painéis tipo "table view" no Grafana.

        Cada registro representa o ÚLTIMO valor coletado de cada campo
        analógico por dispositivo.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_inverter_analogic_data
    Atualização: Manual (ou via scheduled job)

    Casos de Uso:
        - Painéis de status em tempo real no Grafana
        - Tabelas de últimas leituras por inversor
        - Dashboards operacionais

    Formato de Saída:
        device_id | power_plant_id | field_name | value | unit | timestamp

    Exemplo:
        INV-001 | PLANT-A | Frequency | 59.97 | Hz | 2025-12-11 10:30:00

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-11
    ============================================================================
*/

-- ============================================================================
-- OTIMIZAÇÃO: Usa DISTINCT ON (mais eficiente que ROW_NUMBER no PostgreSQL)
-- e CROSS JOIN LATERAL com VALUES para unpivot em um único scan da tabela
-- ============================================================================
WITH latest_per_device AS (
    SELECT DISTINCT ON (device_id)
        device_id,
        power_plant_id,
        timestamp,
        active_power,
        power_reactive,
        power_input,
        power_factor,
        daily_active_energy,
        cumulative_active_energy,
        frequency,
        efficiency,
        current_phase_a,
        current_phase_b,
        current_phase_c,
        line_voltage_ab,
        line_voltage_bc,
        line_voltage_ca,
        string_voltage,
        temperature_internal,
        resistance_insulation,
        state_operation,
        state_simplified
    FROM {{ ref('stg_inverter_analogic_data') }}
    WHERE timestamp > (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 hour'
    ORDER BY device_id, timestamp DESC
),

unpivoted AS (
    SELECT
        ld.device_id,
        ld.power_plant_id,
        v.field_name,
        v.value,
        v.unit,
        ld.timestamp,
        v.display_order
    FROM latest_per_device ld
    CROSS JOIN LATERAL (
        VALUES
            ('Active Power', ld.active_power, 'kW', 1),
            ('Reactive Power', ld.power_reactive, 'kVAr', 2),
            ('Input Power', ld.power_input, 'kW', 3),
            ('Power Factor', ld.power_factor, '', 4),
            ('Rendimento de energia ativa diário', ld.daily_active_energy, 'kWh', 5),
            ('Rendimento de energia ativa acumulada', ld.cumulative_active_energy, 'kWh', 6),
            ('Frequency', ld.frequency, 'Hz', 7),
            ('Efficiency', ld.efficiency, '%', 8),
            ('Current: Phase A', ld.current_phase_a, 'A', 9),
            ('Current: Phase B', ld.current_phase_b, 'A', 10),
            ('Current: Phase C', ld.current_phase_c, 'A', 11),
            ('Voltage: AB line', ld.line_voltage_ab, 'V', 12),
            ('Voltage: BC line', ld.line_voltage_bc, 'V', 13),
            ('Voltage: CA line', ld.line_voltage_ca, 'V', 14),
            ('Tensão String', ld.string_voltage, 'V', 15),
            ('Temperature: internal', ld.temperature_internal, '°C', 16),
            ('Resistance: insulation', ld.resistance_insulation, 'MOhm', 17),
            ('State: Operation', ld.state_operation::DOUBLE PRECISION, '', 18),
            ('State: Simplified', ld.state_simplified::DOUBLE PRECISION, '', 19)
    ) AS v(field_name, value, unit, display_order)
    WHERE v.value IS NOT NULL
)

SELECT
    device_id,
    power_plant_id,
    field_name,
    ROUND(value::numeric, 2) AS value_numeric,
    CONCAT(ROUND(value::numeric, 2), ' ', unit) AS value_formatted,
    unit,
    timestamp,
    display_order
FROM unpivoted
ORDER BY device_id, display_order
