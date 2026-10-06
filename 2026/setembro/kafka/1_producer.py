"""
Produtor Kafka - Transações de Pagamento (simulação)
"""
import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "transacoes.pagamentos"
INTERVALO_SEGUNDOS = 0.5

NOMES = [
    "Ana Silva", "Bruno Costa", "Carla Souza", "Diego Lima", "Elaine Rocha",
    "Fábio Santos", "Gabriela Melo", "Hugo Pereira", "Isabela Nunes", "João Alves",
]
METODOS = ["PIX", "CARTAO_CREDITO", "CARTAO_DEBITO", "BOLETO"]
CATEGORIAS = ["Supermercado", "Restaurante", "Eletronicos", "Combustivel", "Farmacia", "Viagem", "Assinatura"]
CIDADES = ["São Paulo", "Rio de Janeiro", "Curitiba", "Belo Horizonte", "Porto Alegre", "Salvador"]
STATUS_OPCOES = ["APROVADA", "NEGADA", "PENDENTE"]
STATUS_PESOS = [0.85, 0.10, 0.05]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"🚀 Produzindo transações no tópico '{TOPIC}'... (Ctrl+C para parar)\n")

try:
    while True:
        # ~5% das transações vêm com valor alto de propósito, é o que o
        # Consumidor A (detecção de fraude) vai usar pra gerar alerta
        if random.random() < 0.05:
            valor = round(random.uniform(5000, 15000), 2) # 14.005,50
        else:
            valor = round(random.uniform(10, 1200), 2)

        transacao = {
            "transacao_id": str(uuid.uuid4())[:8],
            "cliente": random.choice(NOMES),
            "valor": valor,
            "metodo_pagamento": random.choice(METODOS),
            "categoria": random.choice(CATEGORIAS),
            "cidade": random.choice(CIDADES),
            "status": random.choices(STATUS_OPCOES, weights=STATUS_PESOS)[0],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        producer.send(TOPIC, value=transacao)
        print(f"📡 Enviada: {transacao}")
        time.sleep(INTERVALO_SEGUNDOS)
except KeyboardInterrupt:
    print("\nEncerrando produtor...")
finally:
    producer.close()