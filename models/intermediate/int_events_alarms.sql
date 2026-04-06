{{
    config(
        materialized='incremental',
        unique_key=['id'],
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['id'], 'type': 'btree'},
            {'columns': ['device_id', 'timestamp', 'code'], 'type': 'btree'},
            {'columns': ['power_plant_id', 'timestamp'], 'type': 'btree'},
            {'columns': ['code', 'value'], 'type': 'btree'},
            {'columns': ['type', 'severity'], 'type': 'btree'},
            {'columns': ['acknowledged'], 'type': 'btree'}
        ],
        tags=['int']
    )
}}

/*
    ============================================================================
    INTERMEDIATE MODEL: int_events_alarms
    ============================================================================

    Descrição:
        Processa dados discretos de TODOS os devices e faz join com o catálogo
        de eventos/alarmes para identificar e classificar ocorrências.

        "Explode" o JSON de códigos discretos em linhas individuais e
        enriquece com informações do catálogo (descrição, tipo, severidade).

    Arquitetura:
        Camada: INTERMEDIATE (Gold Layer)
        Fontes: stg_*_discrete_data + tb_event_alarm_catalog
        Destino: int_events_alarms (Tabela regular)
        Atualização: Incremental (a cada 5 minutos)

    Lógica de Negócio:
        1. UNION ALL de dados discretos de todos os devices (inverter, relay, logger, etc)
        2. "Explode" cada código do JSON usando jsonb_each
        3. DETECTA MUDANÇAS DE ESTADO (otimizado para incremental):
           - FULL RUN: Usa LAG() para comparar cada valor com o anterior
           - INCREMENTAL: Busca último estado conhecido na tabela + LAG() nos novos dados
           - Registra apenas quando value != previous_value
           - Inclui primeira ocorrência de cada código (previous_value IS NULL)
        4. JOIN com tb_event_alarm_catalog para enriquecer com:
           - Descrição do evento/alarme
           - Tipo (event/alarm)
           - Severidade (low/medium/high)
        5. Retorna apenas códigos que existem no catálogo E que mudaram de estado

    Otimizações de Performance:
        - Window functions (LAG) são executadas em memória pelo PostgreSQL
        - Em modo incremental, busca apenas último estado (DISTINCT ON)
        - Índices em (device_id, timestamp, code) aceleram ordenação
        - Filtra mudanças de estado ANTES do join com catálogo
        - Reduz drasticamente volume de dados vs. registrar todos os estados

    Dispositivos Incluídos:
        - Inverters (device_type_id = 1) ✅ IMPLEMENTADO
        - Relays (device_type_id = 2) ✅ IMPLEMENTADO
        - Loggers (device_type_id = 3) 🔜 FUTURO
        - Trackers (device_type_id = 4) 🔜 FUTURO
        - Weather Stations (device_type_id = 5) 🔜 FUTURO
        - Meters (device_type_id = 6) 🔜 FUTURO
        - Transformers (device_type_id = 7) 🔜 FUTURO
        - Nobreaks (device_type_id = 8) 🔜 FUTURO

    Casos de Uso:
        - Dashboards de eventos em tempo real
        - Alertas de alarmes críticos
        - Histórico de eventos por device
        - Análise de padrões de falhas por tipo de dispositivo

    Autores: Heitor Teixeira
    Empresa: Axionics Consult
    Data: 2025-12-22
    ============================================================================
*/

WITH discrete_data AS (
    -- INVERTERS (device_type_id = 1)
    SELECT
        timestamp,
        power_plant_id,
        device_id,
        1 AS device_type_id,
        discrete_data_json
    FROM {{ ref('stg_inverter_discrete_data') }}

    WHERE timestamp >= '2026-01-06'::TIMESTAMP -- dados anteriores a 2026/01/06 tinham problemas de qualidade

    {% if is_incremental() %}
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- RELAYS (device_type_id = 2)
    SELECT
        timestamp,
        power_plant_id,
        device_id,
        2 AS device_type_id,
        discrete_data_json
    FROM {{ ref('stg_relay_discrete_data') }}

    WHERE timestamp >= '2026-01-06'::TIMESTAMP -- dados anteriores a 2026/01/06 tinham problemas de qualidade

    {% if is_incremental() %}
        AND timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
),

-- "Explodir" JSON em linhas (uma linha por código)
exploded_codes AS (
    SELECT
        d.timestamp,
        d.power_plant_id,
        d.device_id,
        d.device_type_id,
        code.key AS code,
        code.value AS value
    FROM discrete_data d
    CROSS JOIN LATERAL jsonb_each_text(d.discrete_data_json) AS code
    WHERE code.value IN ('0', '1', 'true', 'false')  -- Valores válidos (binários)
),

-- Em modo incremental, buscar últimos estados conhecidos da tabela
-- Isso é crucial para detectar mudanças de estado corretamente:
-- Se o último registro foi code=100 value=1, e o novo também é value=1,
-- não devemos registrar como evento (não houve mudança)
{% if is_incremental() %}
last_known_states AS (
    SELECT DISTINCT ON (device_id, code)
        device_id,
        code,
        value AS last_value  -- Manter como INTEGER para match de tipo
    FROM {{ this }}
    ORDER BY device_id, code, timestamp DESC
),
{% endif %}

-- Normalizar valores true/false → 1/0
normalized_values AS (
    SELECT
        timestamp,
        power_plant_id,
        device_id,
        device_type_id,
        code,
        CASE
            WHEN value = 'true' THEN 1
            WHEN value = 'false' THEN 0
            ELSE value::INTEGER
        END AS value
    FROM exploded_codes
),

-- Detectar mudanças de estado usando LAG para comparar com valor anterior
-- EXEMPLO: device_id=100, code='overvoltage'
--   timestamp 1: value=0 (previous=NULL) -> REGISTRA (primeira ocorrência)
--   timestamp 2: value=0 (previous=0)    -> IGNORA (sem mudança)
--   timestamp 3: value=1 (previous=0)    -> REGISTRA (mudança 0->1, alarme ativou!)
--   timestamp 4: value=1 (previous=1)    -> IGNORA (sem mudança)
--   timestamp 5: value=0 (previous=1)    -> REGISTRA (mudança 1->0, alarme desativou!)
state_changes AS (
    SELECT
        n.timestamp,
        n.power_plant_id,
        n.device_id,
        n.device_type_id,
        n.code,
        n.value,
        -- Em modo incremental, usar último estado conhecido da tabela
        {% if is_incremental() %}
        COALESCE(
            LAG(n.value) OVER (PARTITION BY n.device_id, n.code ORDER BY n.timestamp),
            lks.last_value
        ) AS previous_value
        FROM normalized_values n
        LEFT JOIN last_known_states lks
            ON lks.device_id = n.device_id
            AND lks.code = n.code
        {% else %}
        LAG(value) OVER (PARTITION BY device_id, code ORDER BY timestamp) AS previous_value
    FROM normalized_values n
        {% endif %}
),

-- Filtrar apenas mudanças de estado (estado atual != estado anterior)
changed_states AS (
    SELECT
        timestamp,
        power_plant_id,
        device_id,
        device_type_id,
        code,
        value
    FROM state_changes
    WHERE
        -- Primeira ocorrência do código (previous_value é NULL)
        previous_value IS NULL
        -- OU houve mudança de estado
        OR value != previous_value
),

-- Join com catálogo de eventos/alarmes
enriched_events AS (
    SELECT
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.device_type_id,
        e.code,
        e.value,  -- Já é INTEGER agora

        -- Campos do catálogo
        c.description,
        c.type,
        c.severity

    FROM changed_states e
    -- change on 2025/01/06 show a high alarm on an unidenfied code
    LEFT JOIN {{ source('raw', 'tb_event_alarm_catalog') }} c
        ON e.code = c.code
        AND e.value = c.value
        AND e.device_type_id = c.device_type_id  -- Match por tipo de device
        AND c.is_active = TRUE
)

SELECT
    --ID unico HASH de timestamp, device_id e code
    ABS(('x' || LEFT(md5(ee.device_id::TEXT || ee.timestamp::TEXT || ee.code::TEXT), 16))::BIT(64)::BIGINT) AS id,
    ee.timestamp,
    ee.power_plant_id,
    pp.name AS power_plant_name,
    ee.device_id,
    d.device_name,
    ee.device_type_id,
    ee.code,
    ee.value,
    -- change on 2025/01/06 show a high alarm on an unidenfied code
    COALESCE(
        ee.description,
        'Código "' || ee.code || '" não cadastrado (device_type_id=' || ee.device_type_id || ', value=' || ee.value || ')'
    ) AS description,
    COALESCE(ee.type, 'alarm') AS type,
    COALESCE(ee.severity, 'high') AS severity,
    FALSE AS acknowledged,
    NULL::TEXT AS acknowledged_by,
    NULL::TIMESTAMP AS acknowledged_at
    
FROM enriched_events ee
LEFT JOIN {{ source('reference', 'tb_devices') }} d
    ON d.id = ee.device_id
LEFT JOIN {{ source('reference', 'tb_power_plants') }} pp
    ON pp.id = ee.power_plant_id
-- Opcional: descomentar para retornar apenas eventos atuados (value = 1)
-- WHERE value = 1
ORDER BY timestamp DESC, device_id, code