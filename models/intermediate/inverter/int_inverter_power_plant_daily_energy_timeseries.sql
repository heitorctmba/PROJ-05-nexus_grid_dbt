{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['date', 'power_plant_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['date', 'power_plant_id'], 'type': 'btree', 'unique': True},
            {'columns': ['power_plant_id'], 'type': 'btree'}
        ],
        tags=['int', 'timeseries', 'daily']
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_inverter_power_plant_daily_energy_timeseries
    ============================================================================

    Descrição:
        Agrega a energia diária gerada por usina, agrupado por dia e power_plant_id.
        Para cada dia, pega o último registro de cada inversor (MAX) e soma
        a energia de todos os inversores daquela usina.

        Para o dia corrente, retorna o acumulado até o momento.

        Ideal para gráficos de barras mostrando o histórico de geração
        diária de uma usina específica.

    Camada: INTERMEDIATE (Silver Layer)
    Fonte: stg_inverter_analogic_data
    Atualização: Incremental (delete+insert por date + power_plant_id)

    Otimizações Aplicadas:
        1. Materialização INCREMENTAL - só processa dias novos
        2. Usa time_bucket do TimescaleDB para particionamento eficiente
        3. Agregação em duas etapas: MAX por inversor, depois SUM por usina
        4. Filtro incremental processa apenas últimos 3 dias (safety margin)

    Lógica:
        1. Agrupa por dia, power_plant_id e device_id, pega MAX(daily_active_energy)
           (último valor do dia = maior valor acumulado)
        2. Soma energia de todos os inversores por usina por dia
        3. Retorna série temporal: date | power_plant_id | total_energy_kwh

    Casos de Uso:
        - Gráfico de barras: histórico de geração diária por usina
        - Página individual de usina no frontend

    Formato de Saída:
        date | power_plant_id | total_energy_kwh | total_active_inverters

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2026-03-23
    ============================================================================
*/

WITH daily_energy_per_inverter AS (
    SELECT
        time_bucket('1 day', timestamp)::DATE AS date,
        power_plant_id,
        device_id,
        MAX(daily_active_energy) AS daily_active_energy
    FROM {{ ref('stg_inverter_analogic_data') }}
    WHERE daily_active_energy IS NOT NULL
    {% if is_incremental() %}
        AND timestamp >= (CURRENT_DATE - INTERVAL '3 days')
    {% endif %}
    GROUP BY 1, 2, 3
),

aggregated_per_power_plant AS (
    SELECT
        date,
        power_plant_id,
        SUM(daily_active_energy) AS total_energy_kwh,
        COUNT(device_id)         AS total_active_inverters
    FROM daily_energy_per_inverter
    GROUP BY date, power_plant_id
)

SELECT
    date,
    power_plant_id,
    ROUND(total_energy_kwh, 2) AS total_energy_kwh,
    total_active_inverters
FROM aggregated_per_power_plant
ORDER BY date, power_plant_id
