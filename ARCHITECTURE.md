# 🏗️ Arquitetura do Projeto - Axionics DBT

## 📂 Estrutura de Diretórios

```
axionics-dbt/
├── 📄 README.md                          # Documentação principal
├── 📄 QUICKSTART.md                      # Guia rápido de início
├── 📄 ARCHITECTURE.md                    # Este arquivo
├── 📄 dbt_project.yml                    # Configuração do projeto DBT
├── 📄 packages.yml                       # Dependências (dbt-utils)
├── 📄 .env.example                       # Template de variáveis de ambiente
│
├── 📁 profiles/
│   └── profiles.yml                      # Configuração de conexão com banco
│
├── 📁 models/
│   ├── 📁 staging/                       # Camada STAGING (normalização)
│   │   └── 📁 inverter/
│   │       ├── _sources.yml              # Definição de sources (raw_inverter)
│   │       ├── schema.yml                # Documentação + testes
│   │       ├── stg_inverter_analogic.sql # Modelo incremental
│   │       └── README.md                 # Documentação do modelo
│   │
│   └── 📁 example/                       # Exemplos do DBT (deletar depois)
│       ├── my_first_dbt_model.sql
│       ├── my_second_dbt_model.sql
│       └── schema.yml
│
├── 📁 macros/
│   └── extract_grid_value.sql            # Macro: valor@timestamp → valor
│
├── 📁 tests/                             # Testes customizados (futuro)
├── 📁 seeds/                             # Dados de seed (futuro)
├── 📁 snapshots/                         # Snapshots (futuro)
└── 📁 analyses/                          # Análises ad-hoc (futuro)
```

---

## 🎯 Arquitetura de Dados (Medallion)

```
┌──────────────────────────────────────────────────────────────────────┐
│                          FONTES DE DADOS                             │
├──────────────────────────────────────────────────────────────────────┤
│  API FastAPI  │  MQTT Broker  │  Busca Ativa  │  Sensores          │
└───────┬───────────────┬───────────────┬───────────────┬──────────────┘
        │               │               │               │
        └───────────────┴───────────────┴───────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    CAMADA RAW (Bronze Layer)                         │
├──────────────────────────────────────────────────────────────────────┤
│  Tabelas: raw_inverter, raw_relay, raw_logger, raw_tracker, etc      │
│  Formato: JSONB (valor@YYYYMMDDHHmmss)                               │
│  Tipo: Hypertables TimescaleDB                                       │
│  Retenção: 1 ano                                                     │
│  Compressão: Após 7 dias                                             │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                   ┌────────────┴────────────┐
                   │     DBT Transform       │
                   │ (extract_grid_value)    │
                   └────────────┬────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   CAMADA STG (Silver Layer)   PARCIAL                │
├──────────────────────────────────────────────────────────────────────┤
│  ✅ stg_inverter_analogic - Dados analógicos de inversores           │
│  🚧 stg_inverter_strings - Correntes das strings (futuro)            │
│  🚧 stg_inverter_discrete - Alarmes e eventos (futuro)               │
│  🚧 stg_relay_analogic - Dados de relés (futuro)                     │
│  🚧 stg_logger_analogic - Dados de smart loggers (futuro)            │
│  🚧 stg_weather_station - Dados meteorológicos (futuro)              │
│  🚧 stg_meter - Dados de medidores (futuro)                          │
│  🚧 stg_tracker - Dados de trackers (futuro)                         │
│                                                                       │
│  Formato: Colunas tipadas (FLOAT, INTEGER, TIMESTAMPTZ)              │
│  Tipo: Hypertables TimescaleDB (via post-hook)                       │
│  Atualização: Incremental (unique_key: device_id + timestamp)        │
│  Retenção: 1 ano                                                     │
│  Compressão: Após 7 dias                                             │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  CAMADA INT (Intermediate - Silver+) 🚧               │
├──────────────────────────────────────────────────────────────────────┤
│  🚧 int_inverter - Inversores + metadados (device_name, cabin)       │
│  🚧 int_events_alarms - Eventos/alarmes + catálogo                   │
│                                                                       │
│  Formato: Colunas + metadados enriquecidos                           │
│  Tipo: Views ou Tables                                               │
│  JOIN com: tb_devices, tb_power_plants, tb_event_alarm_catalog       │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     CAMADA MART (Gold Layer) 🚧                       │
├──────────────────────────────────────────────────────────────────────┤
│  🚧 mart_inverter_performance - Performance Ratio (PR)                │
│  🚧 mart_plant_daily_energy - Energia diária por usina               │
│  🚧 mart_device_availability - Disponibilidade de devices            │
│  🚧 mart_string_heatmap - Análise de strings (heatmap)               │
│                                                                       │
│  Formato: Agregações pré-calculadas                                  │
│  Tipo: Views ou Materialized Views                                   │
│  Consumo: Grafana Dashboards                                         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
                          ┌─────────────┐
                          │   GRAFANA   │
                          │  Dashboards │
                          └─────────────┘
```

---

## 🔧 Componentes Criados

### 1. **Macro: `extract_grid_value`**

**Arquivo:** [macros/extract_grid_value.sql](macros/extract_grid_value.sql)

**Propósito:** Extrair valores numéricos do formato Grid Co (`"valor@YYYYMMDDHHmmss"`).

**Uso:**
```sql
{{ extract_grid_value('json_data', 'active_power') }}
-- Converte: "-517996.544@20251130135055" → -517996.544

{{ extract_grid_value('json_data', 'state_operation', 'INTEGER') }}
-- Converte: "3@20251130143400" → 3
```

**Tratamento de NULL:**
- Retorna `NULL` se campo não existir
- Retorna `NULL` se valor for vazio

---

### 2. **Modelo: `stg_inverter_analogic`**

**Arquivo:** [models/staging/inverter/stg_inverter_analogic.sql](models/staging/inverter/stg_inverter_analogic.sql)

**Propósito:** Normalizar dados analógicos de inversores.

**Características:**
- ✅ Materialização: `incremental`
- ✅ Unique key: `[device_id, timestamp]`
- ✅ Hypertable TimescaleDB (chunk_time_interval = 1 day)
- ✅ Compressão automática após 7 dias
- ✅ Retenção de 1 ano
- ✅ Índices: `(device_id, timestamp)` e `(power_plant_id, timestamp)`

**Campos extraídos (18 campos):**

| Categoria | Campos |
|-----------|--------|
| **Identificadores** | timestamp, power_plant_id, device_id |
| **Potência/Energia** | active_power, power_reactive, power_input, power_factor, daily_active_energy, cumulative_active_energy |
| **Elétrica** | frequency, efficiency, current_phase_a/b/c, line_voltage_ab/bc/ca, string_voltage |
| **Temperatura** | temperature_internal, resistance_insulation |
| **Estado** | state_operation, state_simplified |

**Campos NÃO incluídos:**
- ❌ Strings (string_1_current até string_N_current)
- ❌ Alarmes discretos (códigos 2001-2106)

---

### 3. **Source: `raw_inverter`**

**Arquivo:** [models/staging/inverter/_sources.yml](models/staging/inverter/_sources.yml)

**Propósito:** Definir source da tabela `raw_inverter`.

**Freshness:**
- ⚠️ Warning: Após 10 minutos
- ❌ Error: Após 30 minutos

---

### 4. **Schema: Documentação + Testes**

**Arquivo:** [models/staging/inverter/schema.yml](models/staging/inverter/schema.yml)

**Testes incluídos:**
- `not_null` - Campos obrigatórios
- `accepted_range` (via dbt-utils) - Validação de faixas
- `relationships` - Integridade referencial
- `accepted_values` - Valores permitidos

**Exemplo:**
```yaml
- name: frequency
  tests:
    - dbt_utils.accepted_range:
        min_value: 59.0
        max_value: 61.0
```

---

## 🔄 Fluxo de Execução

### Primeira Execução (Full Refresh)

```bash
dbt run --models stg_inverter_analogic --full-refresh
```

**O que acontece:**

1. ✅ Cria extensão TimescaleDB (pre-hook)
2. ✅ Cria tabela `stg_inverter_analogic`
3. ✅ Processa TODOS os dados de `raw_inverter`
4. ✅ Converte para hypertable (post-hook)
5. ✅ Adiciona políticas de compressão e retenção (post-hook)

### Execução Incremental (Padrão)

```bash
dbt run --models stg_inverter_analogic
```

**O que acontece:**

1. ✅ Identifica `MAX(timestamp)` da tabela staging
2. ✅ Processa apenas dados com `timestamp > MAX(timestamp)`
3. ✅ Insere novos registros (upsert por unique_key)

**SQL gerado:**
```sql
WHERE timestamp > (SELECT MAX(timestamp) FROM stg_inverter_analogic)
```

---

## 📊 TimescaleDB Features

### Hypertable

```sql
SELECT create_hypertable(
    '{{ this.identifier }}',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);
```

⚠️ **Importante:** Use `{{ this.identifier }}` em vez de `{{ this }}` no `create_hypertable()`, pois o TimescaleDB não aceita nomes qualificados com schema.

**Benefícios:**
- Particionamento automático por dia
- Queries otimizadas por tempo
- Paralelização de queries

### Compressão

```sql
ALTER TABLE stg_inverter_analogic SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('stg_inverter_analogic', INTERVAL '7 days');
```

**Benefícios:**
- Economia de **90%+** em armazenamento
- Compressão por device_id (chunks independentes)
- Automático após 7 dias

### Retenção

```sql
SELECT add_retention_policy('stg_inverter_analogic', INTERVAL '1 year');
```

**Benefícios:**
- Deleta automaticamente dados > 1 ano
- Economia de armazenamento
- Manutenção zero

---

## 🧪 Validação e Testes

### Executar testes

```bash
# Todos os testes
dbt test --models stg_inverter_analogic

# Apenas testes de schema
dbt test --models stg_inverter_analogic --select test_type:schema

# Apenas testes de data
dbt test --models stg_inverter_analogic --select test_type:data
```

### Testes aplicados

| Campo | Testes |
|-------|--------|
| timestamp | not_null, accepted_range |
| device_id | not_null, relationships |
| active_power | not_null |
| frequency | accepted_range (59-61 Hz) |
| efficiency | accepted_range (0-100%) |
| temperature_internal | accepted_range (-10 a 100°C) |
| state_operation | not_null, accepted_values ([0-5]) |

---

## 📈 Monitoramento

### Verificar dados incrementais

```sql
-- Último timestamp processado
SELECT MAX(timestamp) AS last_processed
FROM stg_inverter_analogic;

-- Registros por dia
SELECT
    DATE(timestamp) AS day,
    COUNT(*) AS records,
    COUNT(DISTINCT device_id) AS devices
FROM stg_inverter_analogic
GROUP BY DATE(timestamp)
ORDER BY day DESC
LIMIT 7;
```

### Verificar compressão

```sql
-- Chunks comprimidos
SELECT
    chunk_name,
    range_start,
    range_end,
    is_compressed,
    pg_size_pretty(before_compression_total_bytes) AS before,
    pg_size_pretty(after_compression_total_bytes) AS after,
    ROUND(100.0 * (1 - after_compression_total_bytes::FLOAT / before_compression_total_bytes::FLOAT), 2) AS compression_ratio
FROM timescaledb_information.compressed_chunk_stats
WHERE hypertable_name = 'stg_inverter_analogic'
ORDER BY range_start DESC;
```

---
