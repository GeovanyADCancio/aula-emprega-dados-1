"""
Consumidor A - Detecção de Fraude (tempo real)

auto_offset_reset='latest': só processa transações a partir de agora, pois 
para um alerta em tempo real, não interessa reprocessar histórico.
"""
import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "transacoes.pagamentos"
GROUP_ID = "deteccao-fraude"
LIMITE_VALOR_SUSPEITO = 5000

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
)

print("🛡️  Consumidor A (Detecção de Fraude) iniciado.\n")

for msg in consumer:
    t = msg.value
    if t["valor"] > LIMITE_VALOR_SUSPEITO:
        print(f"[A] 🚨 ALERTA valor suspeito: {t['cliente']} - R$ {t['valor']:.2f} ({t['metodo_pagamento']}, {t['cidade']})")
    elif t["status"] == "NEGADA":
        print(f"[A] ⚠️  Transação negada: {t['cliente']} - R$ {t['valor']:.2f}")
    # else:
    #     print(f"[A] OK: {t['cliente']} - R$ {t['valor']:.2f} aprovada")