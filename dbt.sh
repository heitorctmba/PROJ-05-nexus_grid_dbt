#!/bin/bash

# Script para executar DBT com variáveis de ambiente carregadas
# Uso: ./dbt.sh [comando dbt] [argumentos]
# Exemplos:
#   ./dbt.sh debug
#   ./dbt.sh run --target prod
#   ./dbt.sh test --select stg_inverter_analogic

if [ ! -f .env ]; then
    echo "❌ Erro: Arquivo .env não encontrado!"
    echo "💡 Crie o arquivo .env baseado no .env.example"
    exit 1
fi

echo "📦 Carregando variáveis de ambiente do .env..."
export $(grep -v '^#' .env | grep -v '^$' | xargs)

if [ $# -eq 0 ]; then
    echo "❌ Erro: Nenhum comando especificado!"
    echo ""
    echo "Uso: ./dbt.sh [comando] [argumentos]"
    echo ""
    echo "Exemplos:"
    echo "  ./dbt.sh debug"
    echo "  ./dbt.sh run"
    echo "  ./dbt.sh run --target prod"
    echo "  ./dbt.sh test --select stg_inverter_analogic"
    echo "  ./dbt.sh build --target prod"
    exit 1
fi

# ─── Monitor de pg_stat_activity ─────────────────────────────────────────────
# Ativa automaticamente para run/build/test — printa queries ativas a cada 5s.
MONITOR_PID=""

DBT_CMD="$1"
if [[ "$DBT_CMD" == "run" || "$DBT_CMD" == "build" || "$DBT_CMD" == "test" ]]; then

    # Extrai o --target dos argumentos para o monitor saber qual banco consultar
    MONITOR_TARGET="local"
    for arg in "$@"; do
        if [[ "$PREV_ARG" == "--target" ]]; then
            MONITOR_TARGET="$arg"
        fi
        PREV_ARG="$arg"
    done

    DBT_MONITOR_TARGET="$MONITOR_TARGET" python3 "$(dirname "$0")/monitor_dbt.py" &
    MONITOR_PID=$!
fi

# ─── Execução do dbt ──────────────────────────────────────────────────────────
echo "🚀 Executando: uv run dbt $@"
echo ""
uv run dbt "$@" --profiles-dir ./profiles
DBT_EXIT=$?

# ─── Encerra o monitor ────────────────────────────────────────────────────────
if [ -n "$MONITOR_PID" ]; then
    kill "$MONITOR_PID" 2>/dev/null
    wait "$MONITOR_PID" 2>/dev/null
    echo ""
fi

exit $DBT_EXIT
