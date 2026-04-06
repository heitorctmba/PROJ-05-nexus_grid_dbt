#!/usr/bin/env python3
"""
Monitor de execução dbt — consulta pg_stat_activity a cada 5s
e exibe as queries ativas enquanto o dbt roda.

Iniciado em background por dbt.sh automaticamente nos comandos run/build/test.
"""

import os
import sys
import time
import signal

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    # psycopg2 não instalado — monitor encerra silenciosamente
    sys.exit(0)

POLL_INTERVAL = 5  # segundos entre cada consulta

# Lê credenciais do ambiente (já exportadas pelo dbt.sh)
def get_conn_params():
    target = os.environ.get("DBT_MONITOR_TARGET", "prod")
    if target == "prod":
        return {
            "host":     os.environ.get("DB_HOST_PROD"),
            "port":     int(os.environ.get("DB_PORT_PROD", 5432)),
            "dbname":   os.environ.get("DBT_DB_NAME_PROD"),
            "user":     os.environ.get("DBT_DB_USER_PROD"),
            "password": os.environ.get("DBT_DB_PASSWORD_PROD"),
        }
    elif target == "dev":
        return {
            "host":     os.environ.get("DB_HOST_DEV"),
            "port":     int(os.environ.get("DB_PORT_DEV", 5432)),
            "dbname":   os.environ.get("DBT_DB_NAME_DEV"),
            "user":     os.environ.get("DBT_DB_USER_DEV"),
            "password": os.environ.get("DBT_DB_PASSWORD_DEV"),
        }
    else:
        return {
            "host":     os.environ.get("DB_HOST_LOCAL", "localhost"),
            "port":     int(os.environ.get("DB_PORT_LOCAL", 5432)),
            "dbname":   os.environ.get("DBT_DB_NAME_LOCAL", "powerplants"),
            "user":     os.environ.get("DBT_DB_USER_LOCAL", "postgres"),
            "password": os.environ.get("DBT_DB_PASSWORD_LOCAL", "postgres"),
        }

QUERY_ACTIVITY = """
    SELECT
        pid,
        state,
        ROUND(EXTRACT(EPOCH FROM (NOW() - query_start))::numeric, 1)  AS duration_sec,
        COALESCE(wait_event_type || '/' || wait_event, '—')            AS wait,
        query
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query NOT LIKE '%pg_stat_activity%'
      AND query NOT LIKE '%pg_sleep%'
      AND query_start IS NOT NULL
    ORDER BY query_start
"""

def shorten(text: str, max_len: int = 120) -> str:
    text = " ".join(text.split())  # normaliza espaços/newlines
    return text if len(text) <= max_len else text[:max_len] + "…"

def format_duration(sec: float) -> str:
    if sec < 60:
        return f"{sec:.0f}s"
    m, s = divmod(int(sec), 60)
    return f"{m}m{s:02d}s"

def run_monitor():
    params = get_conn_params()

    # Valida que temos credenciais
    if not params.get("host"):
        sys.exit(0)

    try:
        conn = psycopg2.connect(connect_timeout=5, **params)
        conn.autocommit = True
    except Exception:
        sys.exit(0)

    cur = conn.cursor(cursor_factory=RealDictCursor)
    tick = 0

    def handle_exit(sig, frame):
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    print("\n", flush=True)

    while True:
        try:
            cur.execute(QUERY_ACTIVITY)
            rows = cur.fetchall()
        except Exception:
            break

        tick += 1
        ts = time.strftime("%H:%M:%S")

        if not rows:
            if tick % 3 == 0:  # imprime "idle" só a cada 15s para não poluir
                print(f"\033[90m[{ts}] 💤  banco idle — aguardando próxima query...\033[0m", flush=True)
        else:
            for row in rows:
                dur   = format_duration(float(row["duration_sec"]))
                wait  = row["wait"]
                query = shorten(row["query"])
                pid   = row["pid"]

                # Colorização básica por duração
                if float(row["duration_sec"]) > 60:
                    color = "\033[33m"   # amarelo
                elif float(row["duration_sec"]) > 10:
                    color = "\033[36m"   # ciano
                else:
                    color = "\033[32m"   # verde

                print(
                    f"{color}[{ts}] ⏱  {dur:>6}  pid={pid}  wait={wait}\033[0m\n"
                    f"         \033[90m↳ {query}\033[0m",
                    flush=True
                )

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run_monitor()
