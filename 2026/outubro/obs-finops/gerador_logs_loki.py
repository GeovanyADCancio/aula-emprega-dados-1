"""
Gerador de logs do AIRFLOW enviados direto para o Loki (sem Alloy).

Simula o orquestrador de uma plataforma de dados: DAGs que leem do Kafka,
processam com Spark, gravam no S3 e consultam o Postgres. Quando algo
quebra, o Airflow registra o erro. É isso que o Grafana vai mostrar.

Uso:  pip install requests
      python3 gerador_logs_loki.py        (Ctrl+C para parar)
"""
import random
import time
from datetime import datetime, timedelta

import requests

# ----------------------------------------------------------------------------
# CONFIGURAÇÃO
# ----------------------------------------------------------------------------
LOKI_URL = "http://localhost:3100/loki/api/v1/push"
AMBIENTE = "prd"
HISTORICO_MIN = 120        # no início, gera as últimas 2h para o dashboard já ter dados
INTERVALO_S = 5            # depois, gera logs novos a cada 5s

# DAG -> tasks (o fluxo do pipeline)
DAGS = {
    "ingestao_pedidos":      ["ler_kafka", "validar_schema", "gravar_bronze_s3"],
    "transformacao_silver":  ["submeter_spark", "checar_qualidade", "gravar_silver_s3"],
    "carga_gold_financeiro": ["consultar_postgres", "submeter_spark", "gravar_gold_s3"],
}

# Erros possíveis por tipo de task: (código do erro, mensagem como apareceria no log)
ERROS = {
    "ler_kafka": [
        ("kafka_consumer_lag",     "Consumer lag acima do limite no topico pedidos (lag=482113)"),
        ("kafka_broker_indisp",    "Broker kafka-02:9092 indisponivel, metadata timeout"),
    ],
    "validar_schema": [
        ("schema_incompativel",    "Campo valor_total chegou como string, esperado decimal"),
    ],
    "submeter_spark": [
        ("spark_oom",              "java.lang.OutOfMemoryError: Java heap space no executor 7"),
        ("spark_executor_perdido", "ExecutorLostFailure: executor 3 perdido (spot reclaim)"),
    ],
    "checar_qualidade": [
        ("qualidade_nulos",        "Coluna cliente_id com 12.4% de nulos (limite 1%)"),
    ],
    "consultar_postgres": [
        ("postgres_conexoes",      "FATAL: remaining connection slots are reserved"),
    ],
    "gravar_bronze_s3":  [("s3_acesso_negado", "AccessDenied ao gravar em s3://lake-prd/bronze/pedidos/"),
                          ("s3_timeout",       "ReadTimeout ao gravar em s3://lake-prd/bronze/pedidos/")],
    "gravar_silver_s3":  [("s3_timeout",       "ReadTimeout ao gravar em s3://lake-prd/silver/pedidos/")],
    "gravar_gold_s3":    [("s3_timeout",       "ReadTimeout ao gravar em s3://lake-prd/gold/financeiro/")],
}


def em_incidente(ts: datetime) -> bool:
    """Minutos 40 a 49 de cada hora = incidente (o gerador de métricas usa a mesma regra)."""
    return 40 <= ts.minute < 50


# ----------------------------------------------------------------------------
# GERAÇÃO DOS EVENTOS DE UM INSTANTE
# ----------------------------------------------------------------------------
def gerar_eventos(ts: datetime) -> list[tuple[int, str, str]]:
    """Devolve lista de (timestamp_ns, nivel, linha) para um instante."""
    prob_erro = 0.40 if em_incidente(ts) else 0.04
    eventos = []

    for _ in range(random.randint(2, 4)):
        dag = random.choice(list(DAGS))
        task = random.choice(DAGS[dag])
        run_id = f"scheduled__{ts:%Y%m%dT%H%M}"
        base = f"dag={dag} task={task} run_id={run_id}"
        ns = int(ts.timestamp() * 1e9) + random.randint(0, 999_999)

        if random.random() < prob_erro:
            codigo, msg = random.choice(ERROS[task])
            tentativa = random.randint(1, 3)
            eventos.append((ns, "ERROR", f'{base} evento=task_failed erro={codigo} tentativa={tentativa} msg="{msg}"'))
            if tentativa < 3:
                eventos.append((ns + 1_000_000, "WARNING", f'{base} evento=retry_agendado tentativa={tentativa + 1}'))
        else:
            duracao = round(random.uniform(5, 90), 1)
            eventos.append((ns, "INFO", f'{base} evento=task_success duracao_s={duracao}'))

    return eventos


# ----------------------------------------------------------------------------
# ENVIO PARA O LOKI
# Cada combinação de labels vira um "stream". Só 3 labels de baixa cardinalidade;
# dag, task e erro ficam dentro da linha (filtramos com LogQL depois).
# ----------------------------------------------------------------------------
def enviar(eventos: list[tuple[int, str, str]]) -> None:
    por_nivel: dict[str, list] = {}
    for ns, nivel, linha in sorted(eventos):
        por_nivel.setdefault(nivel, []).append([str(ns), linha])

    streams = [
        {"stream": {"ferramenta": "airflow", "ambiente": AMBIENTE, "nivel": nivel}, "values": valores}
        for nivel, valores in por_nivel.items()
    ]
    resp = requests.post(LOKI_URL, json={"streams": streams}, timeout=10)
    resp.raise_for_status()


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    # 1) histórico: um "tick" a cada 15s nas últimas 2h, enviado em lotes
    agora = datetime.now()
    lote = []
    t = agora - timedelta(minutes=HISTORICO_MIN)
    while t < agora:
        lote += gerar_eventos(t)
        if len(lote) > 2000:
            enviar(lote)
            lote = []
        t += timedelta(seconds=15)
    if lote:
        enviar(lote)
    print(f"Histórico de {HISTORICO_MIN} min enviado ao Loki.")

    # 2) tempo real
    print("Gerando logs em tempo real (Ctrl+C para parar)...")
    while True:
        enviar(gerar_eventos(datetime.now()))
        time.sleep(INTERVALO_S)