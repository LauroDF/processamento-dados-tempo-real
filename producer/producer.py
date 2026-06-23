import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker("pt_BR")

TOPIC = "vendas_raw"

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

categorias = [
    "Eletrônicos",
    "Livros",
    "Roupas",
    "Casa",
    "Esportes"
]

def gerar_transacao():

    # 90% normais
    # 10% alto valor

    if random.random() < 0.10:
        valor = round(random.uniform(501, 5000), 2)
    else:
        valor = round(random.uniform(10, 500), 2)

    return {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": random.randint(1, 1000),
        "valor_total": valor,
        "categoria": random.choice(categorias),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

print("Iniciando produtor...")

while True:

    transacao = gerar_transacao()

    producer.send(TOPIC, transacao)
    producer.flush()

    print(
        f"Enviado -> "
        f"{transacao['categoria']} | "
        f"R$ {transacao['valor_total']}"
    )

    time.sleep(1)