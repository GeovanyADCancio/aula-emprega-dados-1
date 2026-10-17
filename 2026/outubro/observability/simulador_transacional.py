"""
Simulador Transacional (OLTP) - e-commerce no Postgres
TERMINAL 1: ambiente transacional. O Debezium captura cada mudança daqui
sem que este script saiba que o CDC existe.

Ciclo de vida do pedido: CRIADO -> PAGO -> ENVIADO -> ENTREGUE (ou CANCELADO).
Aos INCIDENTE_APOS_SEGUNDOS simula uma falha no gateway de pagamento
(pico de cancelamentos) para o monitor analítico disparar alerta.
"""
import random
import time
from datetime import datetime

import psycopg2

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "mydb"
PG_USER = "postgres1"
PG_PASSWORD = "postgres1"

INTERVALO_SEGUNDOS = 0.3
PROB_CANCELAMENTO = 0.10
INCIDENTE_APOS_SEGUNDOS = 60  # None desliga o incidente
INCIDENTE_DURACAO_SEGUNDOS = 30
PROB_CANCELAMENTO_INCIDENTE = 0.80

CLIENTES = [
    "Ana Silva", "Bruno Costa", "Carla Souza", "Diego Lima", "Elaine Rocha",
    "Fábio Santos", "Gabriela Melo", "Hugo Pereira", "Isabela Nunes", "João Alves",
    "Karina Dias", "Lucas Ferreira", "Marina Castro", "Nelson Barros", "Olívia Ramos",
]
PRODUTOS = {
    "Notebook": (2500, 7000),
    "Smartphone": (900, 4500),
    "Monitor": (700, 2200),
    "Cadeira Gamer": (900, 2500),
    "Teclado Mecânico": (150, 600),
    "Fone Bluetooth": (80, 500),
    "Webcam": (120, 450),
}
ACOES = ["novo_pedido", "decidir_pagamento", "enviar", "entregar", "purgar_cancelado"]
PESOS = [0.40, 0.30, 0.15, 0.12, 0.03]
TRANSICOES = {"enviar": ("PAGO", "ENVIADO"), "entregar": ("ENVIADO", "ENTREGUE")}

conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
conn.autocommit = True  # cada comando = 1 transação = 1 evento no CDC
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS pedidos (
        pedido_id     SERIAL PRIMARY KEY,
        cliente       VARCHAR(80)   NOT NULL,
        produto       VARCHAR(80)   NOT NULL,
        valor         NUMERIC(10,2) NOT NULL,
        status        VARCHAR(20)   NOT NULL DEFAULT 'CRIADO',
        criado_em     TIMESTAMP     NOT NULL DEFAULT now(),
        atualizado_em TIMESTAMP     NOT NULL DEFAULT now()
    )
""")
# Sem REPLICA IDENTITY FULL o Postgres só grava a chave primária no WAL e o
# campo "before" dos UPDATE/DELETE chega vazio no Debezium.
cur.execute("ALTER TABLE pedidos REPLICA IDENTITY FULL")

print("🏪 Simulador transacional iniciado (Ctrl+C para parar)\n")

inicio = time.time()
estava_em_incidente = False

try:
    while True:
        decorrido = time.time() - inicio
        em_incidente = (
            INCIDENTE_APOS_SEGUNDOS is not None
            and INCIDENTE_APOS_SEGUNDOS <= decorrido < INCIDENTE_APOS_SEGUNDOS + INCIDENTE_DURACAO_SEGUNDOS
        )
        if em_incidente != estava_em_incidente:
            print("\n💥 INCIDENTE: gateway de pagamento instável — cancelamentos disparando\n" if em_incidente
                  else "\n✅ Gateway de pagamento normalizado\n")
            estava_em_incidente = em_incidente

        hora = datetime.now().strftime("%H:%M:%S")
        acao = random.choices(ACOES, weights=PESOS)[0]

        if acao == "novo_pedido":
            produto = random.choice(list(PRODUTOS))
            cliente = random.choice(CLIENTES)
            valor = round(random.uniform(*PRODUTOS[produto]), 2)
            cur.execute(
                "INSERT INTO pedidos (cliente, produto, valor) VALUES (%s, %s, %s) RETURNING pedido_id",
                (cliente, produto, valor),
            )
            pedido_id = cur.fetchone()[0]
            print(f"[{hora}] INSERT INTO pedidos ... -> #{pedido_id} | {cliente} | {produto} | R$ {valor:.2f}")

        elif acao == "decidir_pagamento":
            prob = PROB_CANCELAMENTO_INCIDENTE if em_incidente else PROB_CANCELAMENTO
            novo_status = "CANCELADO" if random.random() < prob else "PAGO"
            cur.execute(
                """
                UPDATE pedidos SET status = %s, atualizado_em = now()
                WHERE pedido_id = (SELECT pedido_id FROM pedidos WHERE status = 'CRIADO' ORDER BY random() LIMIT 1)
                RETURNING pedido_id
                """,
                (novo_status,),
            )
            linha = cur.fetchone()
            if linha:
                print(f"[{hora}] UPDATE pedidos SET status='{novo_status}' WHERE pedido_id={linha[0]}")

        elif acao in TRANSICOES:
            de, para = TRANSICOES[acao]
            cur.execute(
                """
                UPDATE pedidos SET status = %s, atualizado_em = now()
                WHERE pedido_id = (SELECT pedido_id FROM pedidos WHERE status = %s ORDER BY random() LIMIT 1)
                RETURNING pedido_id
                """,
                (para, de),
            )
            linha = cur.fetchone()
            if linha:
                print(f"[{hora}] UPDATE pedidos SET status='{para}' WHERE pedido_id={linha[0]}")

        elif acao == "purgar_cancelado":
            cur.execute(
                """
                DELETE FROM pedidos
                WHERE pedido_id = (SELECT pedido_id FROM pedidos WHERE status = 'CANCELADO' ORDER BY random() LIMIT 1)
                RETURNING pedido_id
                """
            )
            linha = cur.fetchone()
            if linha:
                print(f"[{hora}] DELETE FROM pedidos WHERE pedido_id={linha[0]}")

        time.sleep(INTERVALO_SEGUNDOS)
except KeyboardInterrupt:
    print("\nEncerrando simulador...")
finally:
    cur.close()
    conn.close()