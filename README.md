# 🌞 Axionics DBT - Sistema de Observabilidade de Usinas Solares

**Transformações de dados em camadas para sistema de observabilidade de usinas fotovoltaicas**

Arquitetura: **RAW → STG → INT → MART**

---

## 📋 Visão Geral

Este projeto DBT implementa as transformações de dados do sistema de observabilidade de usinas solares da Grid Co., processando dados de:

- ⚡ Inversores (inverters)
- 🔌 Relés de proteção (relays)
- 📡 Smart Loggers
- 🌐 Trackers solares
- 🌤️ Estações meteorológicas
- 📊 Medidores de faturamento
- 🔧 Transformadores

---

## 🏗️ Arquitetura de Dados

```
┌──────────┐      ┌─────────┐      ┌──────────┐      ┌─────────┐
│ raw_*    │────▶│   STG   │────▶│   INT    │────▶│  MART   │
│ (Bronze) │      │(Silver) │      │(Silver+) │      │ (Gold)  │
└──────────┘      └─────────┘      └──────────┘      └─────────┘
   JSON          Normalizado        Enriquecido      Agregado
   valor@ts      → valores          + metadados      Grafana
```

### Camadas Implementadas

#### 1. **RAW (Bronze Layer)** ✅
- Dados brutos em JSON
- Formato Grid Co: `"valor@YYYYMMDDHHmmss"`
- Hypertables TimescaleDB
- **Fonte:** API FastAPI + MQTT

#### 2. **STG (Staging - Silver Layer)** ✅ (parcial)
- Normalização de JSON → colunas
- Extração de valores numéricos
- Separação: analógico vs. discreto
- **Modelos criados:**
  - ✅ `stg_inverter_analogic` - Dados analógicos de inversores

#### 3. **INT (Intermediate - Silver+)** 🚧 (futuro)
- Dados validados e enriquecidos
- JOIN com tabelas de referência
- Adição de metadados (device_name, cabin, fabricante)

#### 4. **MART (Gold Layer)** 🚧 (futuro)
- Agregações e métricas calculadas
- Performance Ratio (PR)
- Disponibilidade de devices
- Dashboards do Grafana

---

## 🚀 Como Usar

### 1. **Pré-requisitos**

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) - Gerenciador de pacotes Python ultrarrápido
- PostgreSQL 17+ com TimescaleDB 2.x
- Banco de dados `grid_co_observability` criado e populado

### 2. **Instalação do uv**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. **Configuração do Projeto**

```bash
# Navegar para a pasta do projeto DBT
cd axionics-dbt

# Criar arquivo .env com suas credenciais
cp .env.example .env
# Editar .env com suas credenciais

# Instalar dependências do projeto
uv sync

# Tornar o script executável
chmod +x dbt.sh
```

### 4. **Configurar Variáveis de Ambiente**

Edite o arquivo `.env` com suas credenciais

### 5. **Testar Conexão**

```bash
# Testar conexão com o banco
./dbt.sh debug
```

### 6. **Executar Transformações**

```bash
# Executar TODOS os modelos
./dbt.sh run

# Executar apenas modelos staging
./dbt.sh run --models staging

# Executar apenas stg_inverter_analogic
./dbt.sh run --models stg_inverter_analogic

# Full refresh (recriar tabela do zero)
./dbt.sh run --models stg_inverter_analogic --full-refresh
```

### 7. **Testar em Diferentes Ambientes**

O projeto está configurado com dois ambientes: **dev** (desenvolvimento) e **prod** (produção).

```bash
# DESENVOLVIMENTO (padrão)
./dbt.sh debug                     # Testar conexão
./dbt.sh run                       # Executar modelos
./dbt.sh test                      # Executar testes
./dbt.sh build                     # Run + test juntos

# PRODUÇÃO
./dbt.sh run --target prod         # Executar em prod
./dbt.sh test --target prod        # Testar em prod
./dbt.sh build --target prod       # Build completo em prod

# MODELOS ESPECÍFICOS
./dbt.sh run --select staging.*                    # Apenas staging
./dbt.sh run --target prod --select intermediate.* # Intermediate em prod
./dbt.sh test --select stg_inverter_analogic       # Testar modelo específico
```

### 8. **Executar Testes**

```bash
# Executar todos os testes
./dbt.sh test

# Testar apenas stg_inverter_analogic
./dbt.sh test --models stg_inverter_analogic

# Testar em produção
./dbt.sh test --target prod
```

### 9. **Gerar Documentação**

```bash
# Gerar documentação HTML
./dbt.sh docs generate

# Servir documentação (abre navegador)
./dbt.sh docs serve
```

---

## 💡 Sobre o Script `dbt.sh`

O script `dbt.sh` automatiza:
- ✅ Carregamento automático das variáveis do `.env`
- ✅ Execução via `uv run` (sem precisar ativar ambiente virtual)
- ✅ Validação de arquivo `.env` antes de executar
- ✅ Mensagens de erro claras e úteis

**Uso:** `./dbt.sh [comando] [argumentos]`

Qualquer comando DBT pode ser executado através do script!

---

## 📂 Estrutura do Projeto

```
axionics-dbt/
├── models/
│   ├── staging/
│   │   └── inverter/
│   │       ├── _sources.yml          # Definição de sources (raw_inverter)
│   │       ├── schema.yml            # Documentação e testes
│   │       ├── stg_inverter_analogic.sql  # Modelo incremental
│   │       └── README.md             # Documentação do modelo
│   └── example/                      # Modelos de exemplo (deletar)
├── macros/
│   └── extract_grid_value.sql        # Macro para extrair valores Grid Co
├── profiles/
│   └── profiles.yml                  # Configuração de conexão
├── dbt_project.yml                   # Configuração do projeto
├── .env.example                      # Exemplo de variáveis de ambiente
└── README.md                         # Este arquivo
```

---

## 📊 Modelos Disponíveis

### Staging Layer

#### `stg_inverter_analogic` ✅

**Descrição:** Dados analógicos de inversores solares (potência, tensão, corrente, etc).

**Características:**
- Hypertable TimescaleDB
- Atualização incremental (a cada 5 min)
- Compressão automática após 7 dias
- Retenção de 1 ano

**Campos extraídos:**
- Potência (active_power, power_reactive, power_input)
- Energia (daily_active_energy, cumulative_active_energy)
- Elétrica (frequency, efficiency, correntes, tensões)
- Temperatura (temperature_internal, resistance_insulation)
- Estado (state_operation, state_simplified)

**Exemplo de consulta:**

```sql
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

## 🔧 Macros Disponíveis

### `extract_grid_value(json_column, field_name, cast_as='FLOAT')`

Extrai valor numérico do formato Grid Co (`"valor@YYYYMMDDHHmmss"`).

**Exemplos:**

```sql
-- Extrair active_power como FLOAT
{{ extract_grid_value('json_data', 'active_power') }}
-- Input:  "-517996.544@20251130135055"
-- Output: -517996.544

-- Extrair state_operation como INTEGER
{{ extract_grid_value('json_data', 'state_operation', 'INTEGER') }}
-- Input:  "3@20251130143400"
-- Output: 3
```

---

## 🧪 Testes

Os modelos incluem testes automáticos:

- ✅ `not_null` - Campos obrigatórios não podem ser NULL
- ✅ `accepted_range` - Validação de faixas (temperatura, frequência, etc)
- ✅ `relationships` - Integridade referencial com tabelas auxiliares
- ✅ `accepted_values` - Validação de valores permitidos

**Executar testes:**

```bash
dbt test --models stg_inverter_analogic
```

---

## 📈 TimescaleDB Features

### Hypertables

Tabelas otimizadas para séries temporais:

```sql
-- Conversão automática via post-hook do DBT
SELECT create_hypertable(
    '{{ this.identifier }}',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);
```

⚠️ **Importante:** Use `{{ this.identifier }}` em vez de `{{ this }}` no `create_hypertable()`, pois o TimescaleDB não aceita nomes qualificados com schema.

### Compressão Automática

Economia de **90%+** em armazenamento:

```sql
-- Política: comprimir dados após 7 dias
SELECT add_compression_policy('stg_inverter_analogic', INTERVAL '7 days');
```

### Retenção Automática

Dados antigos são deletados automaticamente:

```sql
-- Política: manter apenas 1 ano de dados
SELECT add_retention_policy('stg_inverter_analogic', INTERVAL '1 year');
```

---

## 🔄 Próximos Passos

### Modelos a implementar:

1. **Staging:**
   - [ ] `stg_inverter_strings` - Normalizar correntes das strings
   - [ ] `stg_inverter_discrete` - Extrair alarmes e eventos
   - [ ] `stg_relay_analogic` - Dados de relés de proteção
   - [ ] `stg_logger_analogic` - Dados de smart loggers
   - [ ] `stg_weather_station` - Dados meteorológicos
   - [ ] `stg_meter` - Dados de medidores
   - [ ] `stg_tracker` - Dados de trackers

2. **Intermediate:**
   - [ ] `int_inverter` - Inversores enriquecidos com metadados
   - [ ] `int_events_alarms` - Eventos/alarmes cruzados com catálogo

3. **Mart:**
   - [ ] `mart_inverter_performance` - Performance Ratio por inversor
   - [ ] `mart_plant_daily_energy` - Energia diária por usina
   - [ ] `mart_device_availability` - Disponibilidade de devices
   - [ ] `mart_string_heatmap` - Análise de strings (heatmap)

---