"""
Conciliação Origem x Réplica (CDC)

Reconstrói a tabela pedidos usando SÓ os eventos do tópico (lido desde o início)
e compara, a cada segundo, com um SELECT direto no Postgres.
Pergunta que responde: "o que chegou no analítico é fiel ao que está no banco?"
"""
import json
import os
import time
from collections import Counter

import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "loja.public.pedidos"
# group_id novo a cada execução: o estado fica só em memória, então é preciso reler o tópico inteiro
GROUP_ID = f"conciliacao-{int(time.time())}"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "mydb"
PG_USER = "postgres1"
PG_PASSWORD = "postgres1"

ATUALIZAR_A_CADA_SEGUNDOS = 1
TOLERANCIA_EM_TRANSITO = 5  # eventos que ainda estão a caminho não contam como falha
STATUS = ["CRIADO", "PAGO", "ENVIADO", "ENTREGUE", "CANCELADO"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
)
conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
conn.autocommit = True
cur = conn.cursor()

replica = {}  # pedido_id -> status, construído só com eventos
processados = 0
ultimo_refresh = 0

try:
    while True:
        lote = consumer.poll(timeout_ms=500)
        for registros in lote.values():
            for msg in registros:
                evento = msg.value.get("payload", msg.value)
                if evento["op"] in ("c", "u", "r"):
                    replica[evento["after"]["pedido_id"]] = evento["after"]["status"]
                elif evento["op"] == "d":
                    replica.pop(evento["before"]["pedido_id"], None)
                processados += 1

        agora = time.time()
        if agora - ultimo_refresh < ATUALIZAR_A_CADA_SEGUNDOS:
            continue
        ultimo_refresh = agora

        cur.execute("SELECT pedido_id, status FROM pedidos")
        origem = dict(cur.fetchall())

        divergentes = sum(1 for pid, status in origem.items() if replica.get(pid) != status)
        divergentes += sum(1 for pid in replica if pid not in origem)
        cont_origem = Counter(origem.values())
        cont_replica = Counter(replica.values())

        if divergentes == 0:
            veredito = "🟢 réplica idêntica à origem"
        elif divergentes <= TOLERANCIA_EM_TRANSITO:
            veredito = f"🟢 sincronizada ({divergentes} em trânsito)"
        else:
            veredito = f"🟡 réplica defasada ({divergentes} pedidos divergentes)"

        os.system("cls" if os.name == "nt" else "clear")
        print("═" * 62)
        print(" CONCILIAÇÃO ORIGEM × RÉPLICA  |  Postgres vs. eventos do CDC")
        print("═" * 62)
        print(f" Eventos processados: {processados}\n")
        print(f" {'STATUS':<12}{'POSTGRES':>12}{'RÉPLICA':>12}{'DIFERENÇA':>12}")
        for status in STATUS:
            o = cont_origem.get(status, 0)
            r = cont_replica.get(status, 0)
            print(f" {status:<12}{o:>12}{r:>12}{o - r:>+12}")
        print(f" {'TOTAL':<12}{len(origem):>12}{len(replica):>12}{len(origem) - len(replica):>+12}")
        print(f"\n Pedidos divergentes (ausentes, desatualizados ou a mais): {divergentes}")
        print(f" {veredito}")
except KeyboardInterrupt:
    print("\nEncerrando conciliação...")
finally:
    consumer.close()
    cur.close()
    conn.close()