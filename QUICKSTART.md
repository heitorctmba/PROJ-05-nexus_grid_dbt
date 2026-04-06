# ⚡ Quickstart - Axionics DBT

Guia rápido para começar a usar o projeto DBT em **5 minutos**.

---

## 📋 Pré-requisitos

✅ PostgreSQL 14+ com TimescaleDB instalado
✅ Python 3.11+
✅ Banco de dados `grid_co_observability` criado

---

## 🚀 Passo a Passo

### 1. Configurar ambiente

```bash
# Navegar para a pasta do DBT
cd axionics-dbt

# Criar arquivo .env
cp .env.example .env

# Editar .env com suas credenciais
# DB_HOST=localhost
# DBT_DB_NAME=grid_co_observability
# DBT_DB_USER=postgres
# DBT_DB_PASSWORD=postgres
```

### 2. Instalar DBT

```bash
# Opção 1: Usando uv (recomendado)
uv sync

# Opção 2: Usando pip
pip install dbt-core dbt-postgres
```

### 3. Instalar dependências do DBT

```bash
# Instalar dbt-utils e outros pacotes
dbt deps
```

### 4. Testar conexão

```bash
# Verificar se conecta ao banco
dbt debug
```

**Saída esperada:**
```
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]

Required dependencies:
 - git [OK found]

Connection:
  host: localhost
  port: 5432
  user: postgres
  database: grid_co_observability
  schema: public
  Connection test: [OK connection ok]
```

### 5. Criar banco de dados (se não existe)

```bash
# Voltar para o projeto de observabilidade
cd ../grid-co-observabilidade-usinas/project/estrutura_grid_v1

# Criar schema do banco
psql -U postgres -d grid_co_observability -f create_database.sql

# Popular com dados de teste
python populate_database.py
```

### 6. Executar modelo staging

```bash
# Voltar para o DBT
cd ../../../axionics-dbt

# Executar modelo stg_inverter_analogic
dbt run --models stg_inverter_analogic
```

**Saída esperada:**
```
Running with dbt=1.7.0
Found 1 model, 0 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 1 source, 0 exposures, 0 metrics

Completed successfully

Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

### 7. Executar testes

```bash
# Testar o modelo
dbt test --models stg_inverter_analogic
```

### 8. Verificar dados

```bash
# Conectar ao banco
psql -U postgres -d grid_co_observability

# Consultar dados
SELECT
    timestamp,
    device_id,
    active_power,
    efficiency,
    temperature_internal
FROM stg_inverter_analogic
ORDER BY timestamp DESC
LIMIT 10;
```

---

## 🎯 Próximos Passos

✅ **Concluído!** Você criou seu primeiro modelo staging.

**Agora você pode:**

1. **Agendar execução incremental** (cron, Airflow, etc):
   ```bash
   # Executar a cada 5 minutos
   */5 * * * * cd /path/to/axionics-dbt && dbt run --models stg_inverter_analogic
   ```

2. **Explorar dados no Grafana**:
   - Conectar Grafana ao PostgreSQL
   - Criar dashboards usando `stg_inverter_analogic`

3. **Criar modelos INT e MART**:
   - Enriquecer com metadados (device_name, cabin)
   - Calcular agregações (PR, disponibilidade)

---

## 🐛 Problemas Comuns

### ❌ Erro: "source 'raw.raw_inverter' was not found"

**Solução:** A tabela `raw_inverter` não existe no banco.

```bash
cd ../grid-co-observabilidade-usinas/project/estrutura_grid_v1
psql -U postgres -d grid_co_observability -f create_database.sql
```

### ❌ Erro: "extension timescaledb does not exist"

**Solução:** TimescaleDB não está instalado.

```bash
# Docker
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb:latest-pg14

# Ubuntu/Debian
sudo apt install timescaledb-2-postgresql-14
```

### ❌ Erro: "could not connect to server"

**Solução:** PostgreSQL não está rodando ou .env está errado.

```bash
# Verificar se PostgreSQL está rodando
sudo systemctl status postgresql

# Verificar .env
cat .env
```

---

## 📚 Recursos

- [README completo](README.md) - Documentação completa
- [DBT Docs](https://docs.getdbt.com/) - Documentação oficial do DBT
- [TimescaleDB Docs](https://docs.timescale.com/) - Documentação TimescaleDB

---

**Dúvidas?** Entre em contato:
- jeander@axionicsconsult.com
- gabriel.lins@axionicsconsult.com
