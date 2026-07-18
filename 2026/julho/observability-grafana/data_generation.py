"""
generate_data.py
=================

Gerador de métricas simuladas de nós Kubernetes de uma plataforma de dados
(ex: workers do Airflow, executors do Spark, brokers do Kafka, nós de EMR).

Objetivo didático:
    Simular um cenário realista o suficiente para que os alunos consigam
    construir um dashboard no Grafana com painéis de CPU, memória, disco,
    rede, contagem de pods e alertas de status — sem precisar de um
    cluster Kubernetes real rodando.

O script insere uma nova leitura de métricas por nó a cada N segundos
(INTERVAL_SECONDS) no Postgres, simulando:

    1. Uma carga "base" diferente por tipo de nó (role).
    2. Uma variação natural ao longo do dia (mais uso em horário comercial).
    3. Ruído aleatório (para não parecer uma linha reta no gráfico).
    4. Incidentes: de tempos em tempos, um nó aleatório entra em estado
       "degraded" ou "critical" por alguns minutos, gerando spikes de
       CPU/memória e reinícios de pod — ótimo para ensinar alertas.

Como rodar:
    pip install -r requirements.txt
    python generate_data.py
"""

import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2

# ---------------------------------------------------------------------------
# Configuração de conexão (lê de variáveis de ambiente, com defaults que
# batem com o docker-compose.yml da aula)
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "observability")
DB_USER = os.getenv("DB_USER", "obs_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "obs_pass")

INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))
CLUSTER_NAME = os.getenv("CLUSTER_NAME", "data-platform-cluster")

# Probabilidade, a cada ciclo, de UM nó saudável começar a ter problema
INCIDENT_CHANCE_PER_CYCLE = 0.04
# Duração de um incidente, em número de ciclos de coleta
INCIDENT_DURATION_CYCLES = (6, 18)  # entre ~1min e ~3min, com INTERVAL=10s


# ---------------------------------------------------------------------------
# Definição dos nós simulados
# ---------------------------------------------------------------------------
@dataclass
class NodeProfile:
    """Perfil de carga 'normal' de cada tipo de nó da plataforma de dados."""

    name: str
    role: str
    cpu_baseline: float       # % médio em condições normais
    cpu_volatility: float     # o quanto a CPU varia (ruído)
    mem_baseline: float
    mem_volatility: float
    net_in_baseline: float    # Mbps
    net_out_baseline: float   # Mbps
    pod_count_baseline: int

    # Estado de incidente (controlado pelo simulador em tempo de execução)
    incident_active: bool = field(default=False, repr=False)
    incident_remaining_cycles: int = field(default=0, repr=False)
    incident_severity: str = field(default="healthy", repr=False)


NODES = [
    NodeProfile("airflow-worker-01", "airflow-worker", 35, 8, 45, 6, 12, 8, 6),
    NodeProfile("airflow-worker-02", "airflow-worker", 32, 8, 42, 6, 10, 7, 6),
    NodeProfile("spark-executor-01", "spark-executor", 55, 15, 60, 10, 40, 35, 4),
    NodeProfile("spark-executor-02", "spark-executor", 58, 15, 63, 10, 42, 36, 4),
    NodeProfile("spark-executor-03", "spark-executor", 52, 15, 58, 10, 38, 33, 4),
    NodeProfile("kafka-broker-01", "kafka-broker", 40, 10, 50, 8, 80, 75, 3),
    NodeProfile("kafka-broker-02", "kafka-broker", 38, 10, 48, 8, 78, 72, 3),
    NodeProfile("postgres-metrics-01", "database", 20, 5, 55, 5, 8, 6, 2),
]


# ---------------------------------------------------------------------------
# Funções de simulação
# ---------------------------------------------------------------------------
def business_hours_multiplier() -> float:
    """
    Simula um padrão de uso realista: mais carga durante o horário comercial
    (9h-18h), menos de madrugada. Isso dá um formato de "onda" nos gráficos
    do Grafana em vez de uma linha reta ou ruído puro.
    """
    hour = datetime.now().hour
    if 9 <= hour < 18:
        return 1.25
    if 18 <= hour < 22:
        return 1.05
    return 0.7


def maybe_trigger_incident(node: NodeProfile) -> None:
    """Com uma pequena probabilidade, inicia um incidente num nó saudável."""
    if node.incident_active:
        return
    if random.random() < INCIDENT_CHANCE_PER_CYCLE:
        node.incident_active = True
        node.incident_remaining_cycles = random.randint(*INCIDENT_DURATION_CYCLES)
        node.incident_severity = random.choice(["degraded", "critical"])


def advance_incident(node: NodeProfile) -> None:
    """Avança (ou encerra) um incidente em andamento."""
    if not node.incident_active:
        return
    node.incident_remaining_cycles -= 1
    if node.incident_remaining_cycles <= 0:
        node.incident_active = False
        node.incident_severity = "healthy"


def generate_reading(node: NodeProfile, now: datetime) -> dict:
    """Gera uma leitura de métricas para um nó, considerando hora do dia e incidentes."""
    multiplier = business_hours_multiplier()

    # Ruído normal (gaussiano) em cima do baseline
    cpu = node.cpu_baseline * multiplier + random.gauss(0, node.cpu_volatility)
    mem = node.mem_baseline * multiplier + random.gauss(0, node.mem_volatility)
    disk = 30 + random.gauss(0, 3)  # disco cresce devagar, praticamente estável na aula
    net_in = max(0, node.net_in_baseline * multiplier + random.gauss(0, node.net_in_baseline * 0.2))
    net_out = max(0, node.net_out_baseline * multiplier + random.gauss(0, node.net_out_baseline * 0.2))
    pods = node.pod_count_baseline
    restarts = 0
    status = "healthy"

    if node.incident_active:
        status = node.incident_severity
        if node.incident_severity == "degraded":
            cpu += 25
            mem += 15
            restarts = random.choice([0, 0, 1])
        elif node.incident_severity == "critical":
            cpu += 45
            mem += 30
            net_in *= 0.3  # nó crítico geralmente para de responder direito
            net_out *= 0.3
            restarts = random.choice([1, 1, 2])
            pods = max(0, pods - random.randint(1, 2))
    else:
        # Mesmo saudável, define status "degraded" se ultrapassar limiares,
        # para os alunos verem que o status não é só binário incidente/normal.
        if cpu > 85 or mem > 85:
            status = "degraded"

    # Trava os valores num intervalo plausível de percentual (0-100)
    cpu = min(100, max(0, cpu))
    mem = min(100, max(0, mem))
    disk = min(100, max(0, disk))

    return {
        "timestamp": now,
        "cluster_name": CLUSTER_NAME,
        "node_name": node.name,
        "node_role": node.role,
        "cpu_usage_percent": round(cpu, 2),
        "memory_usage_percent": round(mem, 2),
        "disk_usage_percent": round(disk, 2),
        "network_in_mbps": round(net_in, 2),
        "network_out_mbps": round(net_out, 2),
        "pod_count": pods,
        "pod_restarts": restarts,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------
INSERT_SQL = """
    INSERT INTO node_metrics (
        "timestamp", cluster_name, node_name, node_role,
        cpu_usage_percent, memory_usage_percent, disk_usage_percent,
        network_in_mbps, network_out_mbps, pod_count, pod_restarts, status
    ) VALUES (
        %(timestamp)s, %(cluster_name)s, %(node_name)s, %(node_role)s,
        %(cpu_usage_percent)s, %(memory_usage_percent)s, %(disk_usage_percent)s,
        %(network_in_mbps)s, %(network_out_mbps)s, %(pod_count)s, %(pod_restarts)s, %(status)s
    )
"""


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def main() -> None:
    print(f"[generate_data] Conectando em {DB_HOST}:{DB_PORT}/{DB_NAME} como {DB_USER}...")
    conn = get_connection()
    conn.autocommit = True
    print(f"[generate_data] Conectado. Gerando métricas a cada {INTERVAL_SECONDS}s. Ctrl+C para parar.\n")

    try:
        with conn.cursor() as cur:
            while True:
                now = datetime.now(timezone.utc)
                cycle_readings = []
                for node in NODES:
                    maybe_trigger_incident(node)
                    reading = generate_reading(node, now)
                    cycle_readings.append(reading)
                    cur.execute(INSERT_SQL, reading)
                    advance_incident(node)

                # Log simples no console pra quem estiver acompanhando ao vivo na aula
                alerts = [r for r in cycle_readings if r["status"] != "healthy"]
                ts = now.strftime("%H:%M:%S")
                if alerts:
                    resumo = ", ".join(f"{a['node_name']}={a['status']}" for a in alerts)
                    print(f"[{ts}] {len(cycle_readings)} leituras inseridas | ALERTAS: {resumo}")
                else:
                    print(f"[{ts}] {len(cycle_readings)} leituras inseridas | tudo saudável")

                time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[generate_data] Interrompido pelo usuário. Encerrando conexão.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()