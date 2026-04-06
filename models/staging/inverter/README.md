# 📊 Staging - Inverter Models

## Visão Geral

Esta pasta contém os modelos de staging (camada Silver) para dados de **inversores solares**.

## Modelos Disponíveis

### 1. `stg_inverter_analogic`

**Descrição:** Extrai e normaliza dados analógicos dos inversores.

**Características:**
- ✅ Hypertable TimescaleDB (chunks de 1 dia)
- ✅ Atualização incremental (unique_key: device_id + timestamp)
- ✅ Compressão automática após 7 dias
- ✅ Retenção de 1 ano
- ✅ Conversão automática de formato Grid Co (`valor@timestamp` → `valor`)

**Campos Extraídos:**

| Categoria | Campos |
|-----------|--------|
| **Potência e Energia** | active_power, power_reactive, power_input, power_factor, daily_active_energy, cumulative_active_energy |
| **Parâmetros Elétricos** | frequency, efficiency, current_phase_a/b/c, line_voltage_ab/bc/ca, string_voltage |
| **Temperatura** | temperature_internal, resistance_insulation |
| **Estado** | state_operation, state_simplified |

**Campos NÃO incluídos:**
- ❌ Strings (string_1_current até string_N_current) - será modelo separado futuro
- ❌ Alarmes discretos (códigos 2001-2106) - será modelo `stg_inverter_discrete`

**Exemplo de uso:**

```sql
-- Consultar últimas 24h de dados de um inversor
SELECT
    timestamp,
    device_id,
    active_power,
    efficiency,
    temperature_internal
FROM {{ ref('stg_inverter_analogic') }}
WHERE device_id = 1
  AND timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;
```

---

## Arquitetura de Dados

```
┌─────────────────┐
│  raw_inverter   │  ← Dados brutos (JSON)
│  (Bronze Layer) │     Formato: "valor@YYYYMMDDHHmmss"
└────────┬────────┘
         │
         │ DBT Transformation
         │ (extract_grid_value macro)
         ↓
┌─────────────────────────┐
│ stg_inverter_analogic   │  ← Dados normalizados
│   (Silver Layer)        │     Formato: valores numéricos
│   - Hypertable          │
│   - Incremental         │
│   - Compressão (7d)     │
│   - Retenção (1y)       │
└─────────────────────────┘
```

---

## Configuração do DBT

### Materialização

```yaml
materialized: incremental
unique_key: ['device_id', 'timestamp']
on_schema_change: fail
```

### Índices

```yaml
indexes:
  - columns: ['device_id', 'timestamp']
  - columns: ['power_plant_id', 'timestamp']
```

### TimescaleDB Hypertable

A conversão para hypertable é feita automaticamente via `post_hook`:

```sql
SELECT create_hypertable(
    '{{ this.identifier }}',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);
```

⚠️ **Importante:** Sempre use `{{ this.identifier }}` em vez de `{{ this }}` no `create_hypertable()`, pois o TimescaleDB não aceita nomes qualificados com schema (ex: `public.stg_inverter_analogic`).

**Políticas automáticas:**
- **Compressão:** Após 7 dias (economia de 90%+)
- **Retenção:** 1 ano (dados mais antigos são deletados automaticamente)

---

## Como Executar

### Primeira Execução (Full Refresh)

```bash
# Processar todos os dados históricos
dbt run --models stg_inverter_analogic --full-refresh
```

⚠️ **Atenção:** Full refresh recria a tabela e reprocessa TODOS os dados.

### Execução Incremental (Padrão)

```bash
# Processar apenas dados novos
dbt run --models stg_inverter_analogic
```

Processa apenas dados com `timestamp > MAX(timestamp)` da tabela staging.

### Validação e Testes

```bash
# Executar testes de schema
dbt test --models stg_inverter_analogic
```

**Testes incluídos:**
- `not_null` em campos obrigatórios
- `accepted_range` para valores numéricos (frequência, temperatura, etc)
- `relationships` com tabelas de referência (tb_power_plants, tb_devices)

---

## Macro Utilizada

### `extract_grid_value(json_column, field_name, cast_as='FLOAT')`

Extrai valor numérico do formato Grid Co.

**Exemplos:**
```sql
{{ extract_grid_value('json_data', 'active_power') }}
-- Input:  "-517996.544@20251130135055"
-- Output: -517996.544

{{ extract_grid_value('json_data', 'state_operation', 'INTEGER') }}
-- Input:  "3@20251130143400"
-- Output: 3
```

**Tratamento de NULL:**
- Retorna `NULL` se o campo não existir no JSON
- Retorna `NULL` se o valor for vazio ou inválido

---

## Monitoramento

### Verificar dados incrementais processados

```sql
-- Ver timestamp do último dado processado
SELECT MAX(timestamp) AS last_processed
FROM stg_inverter_analogic;

-- Contar registros por dia
SELECT
    DATE(timestamp) AS day,
    COUNT(*) AS records
FROM stg_inverter_analogic
GROUP BY DATE(timestamp)
ORDER BY day DESC
LIMIT 7;
```

### Verificar compressão TimescaleDB

```sql
-- Ver chunks comprimidos
SELECT
    chunk_name,
    range_start,
    range_end,
    is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'stg_inverter_analogic'
ORDER BY range_start DESC
LIMIT 10;
```

---

## Próximos Passos

1. **Criar `stg_inverter_strings`** - Normalizar dados das strings (string_1_current, etc)
2. **Criar `stg_inverter_discrete`** - Extrair alarmes e eventos (códigos 2001-2106)
3. **Criar camada INT** - Enriquecer com metadados (device_name, cabin, fabricante)
4. **Criar camada MART** - Agregações para Grafana

---

## Autores

- **Jeander Trevia** - jeander@axionicsconsult.com
- **Gabriel Lins** - gabriel.lins@axionicsconsult.com

**Empresa:** Axionics Consult
**Data:** Novembro 2025
