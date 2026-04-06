{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='date',
        on_schema_change='fail',
        indexes=[
            {'columns': ['date'], 'type': 'btree', 'unique': True}
        ],
        tags=['int', 'timeseries', 'daily']
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_inverter_daily_energy_timeseries
    ============================================================================

    Descrição:
        Agrega a energia diária total gerada por TODAS as usinas, agrupado
        por dia. Para cada dia, pega o último registro e soma a energia de
        todos os inversores de todas as usinas.

        Ideal para gráficos de linha mostrando evolução dia a dia da geração
        total do portfólio.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_inverter_analogic_data
    Atualização: Incremental (delete+insert por dia)

    Otimizações Aplicadas:
        1. Materialização INCREMENTAL ao invés de TABLE - só processa dias novos
        2. Usa time_bucket do TimescaleDB para particionamento eficiente
        3. Agregação em duas etapas: MAX por inversor, depois SUM total
        4. Filtro incremental processa apenas últimos 3 dias (safety margin)

    Lógica:
        1. Agrupa por dia e device_id, pega MAX(daily_active_energy)
           (último valor do dia = maior valor acumulado)
        2. Soma energia de todos os inversores por dia
        3. Retorna série temporal: date | total_energy_kwh

    Casos de Uso:
        - Gráfico de linha: eixo X = dia, eixo Y = energia total (kWh)
        - Dashboard executivo com KPI de geração diária total
        - Análise de tendências de geração ao longo do tempo
        - Integração N8N para automações

    Formato de Saída:
        date | total_energy_kwh | total_active_inverters | total_power_plants

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-23
    Atualizado: 2026-01-29 (otimização incremental)
    ============================================================================
*/

-- ============================================================================
-- OTIMIZAÇÃO: Usa MAX() ao invés de DISTINCT ON
-- daily_active_energy é um contador acumulativo - o último valor do dia
-- é sempre o maior valor, então MAX() retorna o mesmo resultado com
-- melhor performance (agregação direta vs ordenação + dedup)
-- ============================================================================
WITH daily_energy_per_inverter AS (
    SELECT
        time_bucket('1 day', timestamp)::DATE AS date,
        power_plant_id,
        device_id,
        MAX(daily_active_energy) AS daily_active_energy
    FROM {{ ref('stg_inverter_analogic_data') }}
    WHERE daily_active_energy IS NOT NULL
    {% if is_incremental() %}
        -- Processa apenas últimos 3 dias (margem de segurança para dados atrasados)
        AND timestamp >= (CURRENT_DATE - INTERVAL '3 days')
    {% endif %}
    GROUP BY 1, 2, 3
),

-- Agrega TUDO: soma energia de todos os inversores de todas as usinas por dia
aggregated_total AS (
    SELECT
        date,
        SUM(daily_active_energy) AS total_energy_kwh,
        COUNT(device_id) AS total_active_inverters,
        COUNT(DISTINCT power_plant_id) AS total_power_plants
    FROM daily_energy_per_inverter
    GROUP BY date
)

SELECT
    date,
    ROUND(total_energy_kwh, 2) AS total_energy_kwh,
    total_active_inverters,
    total_power_plants
FROM aggregated_total
ORDER BY date
