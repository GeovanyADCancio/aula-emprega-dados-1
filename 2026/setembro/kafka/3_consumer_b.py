"""
Consumidor B - Persistência (Data Lake local)
Rode num terminal dedicado, junto com o produtor e o Consumidor A.

auto_offset_reset='earliest': lê o tópico desde o início. Como está num
group_id diferente do Consumidor A, cada um mantém seu próprio offset —
os dois recebem TODAS as mensagens, de forma independente um do outro.
"""
import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "transacoes.pagamentos"
GROUP_ID = "persistencia-datalake"
ARQUIVO_SAIDA = "datalake_transacoes.jsonl"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
)

print(f"💾 Consumidor B (Persistência) iniciado — gravando em ./{ARQUIVO_SAIDA}\n")

with open(ARQUIVO_SAIDA, "a", encoding="utf-8") as arquivo:
    for msg in consumer:
        t = msg.value
        arquivo.write(json.dumps(t, ensure_ascii=False) + "\n")
        arquivo.flush()  # sem isso, o arquivo só grava de verdade no disco quando o processo fechar
        print(f"[B] Gravado: {t['transacao_id']} - {t['cliente']} - R$ {t['valor']:.2f}")