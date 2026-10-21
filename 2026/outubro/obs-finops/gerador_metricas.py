"""
Gerador de MÉTRICAS de 4 ferramentas da plataforma de dados, gravadas no Postgres:
    airflow  -> orquestrador (MESMA ferramenta que gera os logs no Loki)
    kafka    -> ingestão em streaming
    spark    -> processamento
    postgres -> banco de apoio

Tabela única em formato "longo": (ts, ferramenta, metrica, valor).
Isso permite adicionar novas ferramentas sem criar colunas.

Uso:  pip install psycopg2-binary
      python3 gerador_metricas.py         (Ctrl+C para parar)
"""
import random
import time
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

# ----------------------------------------------------------------------------
# CONFIGURAÇÃO
# ----------------------------------------------------------------------------
DB = dict(host="localhost", port=5433, user="metricas", password="metricas", dbname="metricas")
HISTORICO_MIN = 120       # histórico inicial (1 ponto por minuto)
INTERVALO_S = 10          # depois, 1 ponto a cada 10s


def em_incidente(ts: datetime) -> bool:
    """Mesma regra do gerador de logs: minutos 40 a 49 de cada hora."""
    return 40 <= ts.minute < 50


# ----------------------------------------------------------------------------
# MEDIÇÕES DE UM INSTANTE
# Em incidente, as métricas pioram nas mesmas ferramentas que aparecem nos logs.
# ----------------------------------------------------------------------------
def medicoes(ts: datetime) -> list[tuple]:
    inc = em_incidente(ts)
    r = random.uniform
    ri = random.randint

    valores = {
        "airflow": {
            "tasks_sucesso":     ri(15, 25) if not inc else ri(8, 14),
            "tasks_falha":       ri(0, 1)   if not inc else ri(6, 12),
            "scheduler_lag_s":   r(1, 3)    if not inc else r(10, 25),
        },
        "kafka": {
            "consumer_lag_msgs": ri(500, 3000)      if not inc else ri(150_000, 480_000),
            "msgs_por_s":        ri(1800, 2200)     if not inc else ri(900, 1300),
        },
        "spark": {
            "executores_ativos": random.choice([8, 8, 8, 7]) if not inc else random.choice([3, 4, 5]),
            "memoria_usada_pct": r(45, 65)          if not inc else r(88, 99),
        },
        "postgres": {
            "conexoes_ativas":   ri(40, 70)         if not inc else ri(92, 100),
            "queries_lentas":    ri(0, 3)           if not inc else ri(10, 30),
        },
    }
    return [(ts, ferr, metrica, round(v, 2))
            for ferr, mets in valores.items() for metrica, v in mets.items()]


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metricas_ferramentas (
            ts         TIMESTAMP NOT NULL,
            ferramenta TEXT      NOT NULL,
            metrica    TEXT      NOT NULL,
            valor      NUMERIC   NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_metricas_ts ON metricas_ferramentas (ts);
    """)

    # Histórico só na primeira execução
    cur.execute("SELECT COUNT(*) FROM metricas_ferramentas")
    if cur.fetchone()[0] == 0:
        agora = datetime.now().replace(second=0, microsecond=0)
        linhas = []
        for i in range(HISTORICO_MIN, 0, -1):
            linhas += medicoes(agora - timedelta(minutes=i))
        execute_values(cur, "INSERT INTO metricas_ferramentas VALUES %s", linhas)
        print(f"Histórico de {HISTORICO_MIN} min gravado.")

    print("Gerando métricas em tempo real (Ctrl+C para parar)...")
    while True:
        execute_values(cur, "INSERT INTO metricas_ferramentas VALUES %s", medicoes(datetime.now()))
        time.sleep(INTERVALO_S)