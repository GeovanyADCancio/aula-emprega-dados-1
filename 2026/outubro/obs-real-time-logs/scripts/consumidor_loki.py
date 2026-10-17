"""
Lê logs do Loki em (quase) tempo real e imprime na tela, em loop.
Equivalente ao consumidor Kafka.
"""
import time
from datetime import datetime

import requests

LOKI_URL = "http://localhost:3100"
POLL_SECONDS = 2                 # intervalo entre consultas
LIMIT = 1000

# Descomente UMA seleção (dag_id e task_id são labels do Loki):
# SELECAO = '{dag_id="gerador_logs_pipeline", task_id="extract"}'
# SELECAO = '{dag_id="gerador_logs_pipeline", task_id="transform"}'
# SELECAO = '{dag_id="gerador_logs_pipeline", task_id="load"}'
SELECAO = '{dag_id="gerador_logs_falhas", task_id="chamar_api_instavel"}'
# SELECAO = '{dag_id="gerador_logs_falhas", task_id="chamar_api_instavel", level="ERROR"}'
# SELECAO = '{dag_id="gerador_logs_falhas", task_id="processar_resposta"}'

# Filtro de texto: remove as linhas "::group::..." e "::endgroup::" que o Airflow
# escreve em volta de "Pre task execution logs" e "Post task execution logs"
QUERY = SELECAO + ' !~ "::(end)?group::"'

ultimo_ts = time.time_ns()  # começa em "agora": só mostra logs novos (nanossegundos, como o Loki)

print(f"Consultando {QUERY} em {LOKI_URL} a cada {POLL_SECONDS}s (Ctrl+C para sair)")
try:
    while True:
        try:
            resp = requests.get(
                f"{LOKI_URL}/loki/api/v1/query_range",
                params={
                    "query": QUERY,
                    "start": ultimo_ts + 1,   # só o que chegou depois da última linha lida
                    "end": time.time_ns(),
                    "limit": LIMIT,
                    "direction": "forward",
                },
                timeout=10,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[loki indisponível] {e.__class__.__name__}, tentando de novo...")
            time.sleep(POLL_SECONDS)
            continue

        # o Loki devolve uma lista por stream; junta tudo e ordena por hora
        eventos = [
            (int(ts), stream["stream"], linha)
            for stream in resp.json()["data"]["result"]
            for ts, linha in stream["values"]
        ]
        for ts, labels, linha in sorted(eventos, key=lambda e: e[0]):
            hora = datetime.fromtimestamp(ts / 1e9).strftime("%H:%M:%S")
            print(f"{hora} {labels.get('level', '-'):<7} "
                  f"{labels.get('dag_id', '-')}/{labels.get('task_id', '-')}  {linha}")
            ultimo_ts = ts

        time.sleep(POLL_SECONDS)
except KeyboardInterrupt:
    print("\nEncerrado.")