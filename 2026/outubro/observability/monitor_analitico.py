"""
Monitor Analítico - consome o CDC do Debezium e calcula métricas em tempo real
TERMINAL 2: ambiente analítico. Não faz nenhuma query no Postgres — todo o
estado abaixo é reconstruído apenas a partir dos eventos do tópico.

Métricas: eventos por operação, pedidos por status, faturamento confirmado,
taxa de cancelamento (janela deslizante), throughput, latência fim-a-fim
do pipeline (commit no Postgres -> chegada aqui) e silêncio (sem eventos).

Como ler o código: o loop faz sempre 3 passos
  1) LER os eventos novos do Kafka
  2) ATUALIZAR as variáveis em memória (o "placar")
  3) REDESENHAR o painel (1x por segundo)
"""
import json
import os
import time
from collections import Counter, deque

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "loja.public.pedidos"  # prefixo.schema.tabela, definido no conector Debezium
GROUP_ID = "monitor-analitico"  # o Kafka guarda a posição de leitura por group_id

ATUALIZAR_A_CADA_SEGUNDOS = 1  # frequência de redesenho do painel
JANELA_SEGUNDOS = 30  # taxa de cancelamento, throughput e latência olham só os últimos 30s
LIMITE_TAXA_CANCELAMENTO = 0.40  # acima de 40% de cancelamentos, alerta
MIN_DECISOES_PARA_ALERTA = 5  # evita alertar com amostra pequena (2 de 3 = 67% não significa nada)
LIMITE_LATENCIA_MS = 3000
ALERTA_SEM_EVENTOS_SEGUNDOS = 10

# "latest": começa a ler do fim do tópico (só o que chegar a partir de agora).
# Só vale na 1ª vez do grupo; depois ele retoma da última posição salva.
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # bytes -> dicionário Python
    auto_offset_reset="latest",
)

# ---- O "placar": estado mantido em memória (some quando o monitor fecha) ----
ops = Counter()  # quantos eventos de cada operação: c=INSERT, u=UPDATE, d=DELETE, r=SNAPSHOT
status_atual = {}  # pedido_id -> status. É a "cópia" da tabela, reconstruída só pelos eventos
faturamento = 0.0
# As 3 filas abaixo são as janelas deslizantes: guardam só o que ocorreu nos últimos 30s
decisoes = deque()  # (instante, foi_cancelamento): CRIADO -> PAGO/CANCELADO na janela
eventos_janela = deque()  # instante de cada evento (para calcular throughput)
latencias = deque()  # (instante, latência em ms)
ultimos = deque(maxlen=8)  # maxlen: descarta sozinho o mais antigo ao passar de 8
NOMES_OP = {"c": "INSERT  ", "u": "UPDATE  ", "d": "DELETE  ", "r": "SNAPSHOT"}

inicio = time.time()
ultimo_refresh = 0
ultimo_evento = inicio  # base do alerta de silêncio

try:
    while True:
        # for msg in consumer : bloqueava até a chegada de novas mensagens
        # poll espera no máximo 0,5s e devolve o que chegou (pode vir vazio). Assim o
        # loop nunca trava: o painel segue atualizando e o alerta de silêncio consegue disparar.
        lote = consumer.poll(timeout_ms=500)
        agora = time.time()

        # ---------- PASSO 1 e 2: ler cada evento e atualizar o placar ----------
        for registros in lote.values():  # o poll agrupa as mensagens por partição
            for msg in registros:
                evento = msg.value.get("payload", msg.value)  # aceita com ou sem envelope de schema
                op = evento["op"]
                antes = evento.get("before")  # estado da linha antes (None em INSERT)
                depois = evento.get("after")  # estado da linha depois (None em DELETE)

                ops[op] += 1
                ultimo_evento = agora
                eventos_janela.append(agora)
                # latência = hora atual - hora do commit no Postgres (source.ts_ms), em ms
                latencias.append((agora, agora * 1000 - evento["source"]["ts_ms"]))

                # reconstrói a cópia da tabela: grava o status novo, ou remove se foi DELETE
                # (no DELETE o "depois" é nulo, por isso a chave vem do "antes")
                if op in ("c", "u", "r"):
                    status_atual[depois["pedido_id"]] = depois["status"]
                elif op == "d":
                    status_atual.pop(antes["pedido_id"], None)

                # só conta quando o STATUS mudou de verdade (um UPDATE pode mexer em outra coluna).
                # Detectar "de quê para quê" só é possível porque o evento traz o "antes".
                if op == "u" and antes["status"] != depois["status"]:
                    # "decisão de pagamento" = pedido saiu de CRIADO e virou PAGO ou CANCELADO
                    if antes["status"] == "CRIADO" and depois["status"] in ("PAGO", "CANCELADO"):
                        decisoes.append((agora, depois["status"] == "CANCELADO"))
                    if depois["status"] == "PAGO":
                        faturamento += depois["valor"]

                # monta a linha de "últimos eventos" conforme o tipo de operação
                hora = time.strftime("%H:%M:%S", time.localtime(agora))
                if op == "u":
                    detalhe = f"#{depois['pedido_id']} {antes['status']} → {depois['status']}"
                elif op == "d":
                    detalhe = f"#{antes['pedido_id']} (estava {antes['status']})"
                else:
                    detalhe = f"#{depois['pedido_id']} {depois['cliente']} | {depois['produto']} | R$ {depois['valor']:.2f}"
                ultimos.append(f"{hora}  {NOMES_OP[op]} {detalhe}")

        # ---------- PASSO 3: redesenhar (o loop gira a cada ~0,5s, o painel só 1x por segundo) ----------
        if agora - ultimo_refresh < ATUALIZAR_A_CADA_SEGUNDOS:
            continue  # ainda não deu 1s: pula o resto e volta pro poll
        ultimo_refresh = agora

        # janela deslizante: descarta o que é mais antigo que 30s. As filas estão em ordem
        # cronológica, então basta olhar o primeiro item e remover enquanto for velho.
        corte = agora - JANELA_SEGUNDOS
        while eventos_janela and eventos_janela[0] < corte:
            eventos_janela.popleft()
        while decisoes and decisoes[0][0] < corte:
            decisoes.popleft()
        while latencias and latencias[0][0] < corte:
            latencias.popleft()

        # métricas da janela. Os "if ... else 0" evitam divisão por zero / lista vazia.
        # janela_efetiva: logo após iniciar, a janela ainda não tem 30s de dados.
        janela_efetiva = max(min(JANELA_SEGUNDOS, agora - inicio), 1)
        throughput = len(eventos_janela) / janela_efetiva
        lat_valores = [l for _, l in latencias]
        lat_media = sum(lat_valores) / len(lat_valores) if lat_valores else 0
        lat_max = max(lat_valores) if lat_valores else 0
        total_decisoes = len(decisoes)
        cancelamentos = sum(1 for _, cancelou in decisoes if cancelou)
        taxa_cancel = cancelamentos / total_decisoes if total_decisoes else 0

        # os 3 alertas respondem a 3 perguntas diferentes:
        alertas = []
        # 1) o NEGÓCIO está com problema? (muitos cancelamentos)
        if total_decisoes >= MIN_DECISOES_PARA_ALERTA and taxa_cancel > LIMITE_TAXA_CANCELAMENTO:
            alertas.append(f"🚨 Taxa de cancelamento alta: {taxa_cancel:.0%} (limite {LIMITE_TAXA_CANCELAMENTO:.0%})")
        # 2) os dados PARARAM de chegar? (frescor)
        if agora - ultimo_evento > ALERTA_SEM_EVENTOS_SEGUNDOS:
            alertas.append(f"🚨 Sem eventos há {agora - ultimo_evento:.0f}s — pipeline ou origem parados?")
        # 3) os dados estão chegando ATRASADOS? (saúde do pipeline)
        if lat_max > LIMITE_LATENCIA_MS:
            alertas.append(f"🚨 Latência do pipeline alta: máx {lat_max:.0f} ms (limite {LIMITE_LATENCIA_MS} ms)")

        por_status = Counter(status_atual.values())  # quantos pedidos há em cada status agora
        maior = max(por_status.values(), default=1)  # base para escalar as barras do gráfico
        rodando = int(agora - inicio)

        # ---------- desenho do painel ----------
        os.system("cls" if os.name == "nt" else "clear")
        print("═" * 64)
        print(f" MONITOR ANALÍTICO  |  CDC Debezium → Kafka  |  rodando há {rodando // 60:02d}:{rodando % 60:02d}")
        print("═" * 64)
        print(f" Tópico: {TOPIC}")
        print(f" Eventos: {sum(ops.values())}  (INSERT {ops['c']} | UPDATE {ops['u']} | DELETE {ops['d']} | SNAPSHOT {ops['r']})")
        print(f" Throughput ({JANELA_SEGUNDOS}s): {throughput:.1f} eventos/s")
        print(f" Latência fim-a-fim ({JANELA_SEGUNDOS}s): média {lat_media:.0f} ms | máx {lat_max:.0f} ms")
        print()
        print(" Pedidos por status (estado reconstruído pelos eventos):")
        for status in ("CRIADO", "PAGO", "ENVIADO", "ENTREGUE", "CANCELADO"):
            qtd = por_status.get(status, 0)
            print(f"   {status:<10} {'█' * int(30 * qtd / maior):<30} {qtd}")  # barra proporcional ao maior status
        print()
        print(f" Faturamento confirmado (desde o início do monitor): R$ {faturamento:,.2f}")
        print(f" Taxa de cancelamento ({JANELA_SEGUNDOS}s): {taxa_cancel:.0%}  ({cancelamentos} de {total_decisoes} decisões de pagamento)")
        print()
        print(" ALERTAS:", "✅ nenhum" if not alertas else "")
        for alerta in alertas:
            print("   " + alerta)
        print()
        print(" Últimos eventos:")
        for linha in ultimos:
            print("   " + linha)
except KeyboardInterrupt:
    print("\nEncerrando monitor...")
finally:
    consumer.close()  # sai do grupo e salva a posição de leitura