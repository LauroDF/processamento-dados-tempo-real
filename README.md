# Pipeline de Processamento de Dados em Tempo Real com Kafka, Flink e Valkey

Este repositório contém o ambiente Docker utilizado para o desenvolvimento do trabalho de Big Data e Processamento de Dados em Tempo Real. O ambiente disponibiliza Kafka, Apache Flink, Valkey (Redis), PostgreSQL, MongoDB e ferramentas de monitoramento para construção de um pipeline de análise de transações em streaming.

## Arquitetura do Projeto

```text
Produtor (Python)
        ↓
Kafka (vendas_raw)
        ↓
Apache Flink
        ↓
Valkey (Redis)
        ↓
Redis Insight
```

### Fluxo esperado

1. O produtor gera transações sintéticas continuamente.
2. As transações são publicadas no tópico Kafka `vendas_raw`.
3. Um job do Flink consome os dados do Kafka.
4. Transações suspeitas (ex.: valor_total > 500) são identificadas.
5. Os alertas são armazenados no Valkey.
6. Os resultados podem ser visualizados pelo Redis Insight.

---

## Interfaces Disponíveis

| Interface           | URL                    |
| ------------------- | ---------------------- |
| Flink Dashboard     | http://localhost:8081  |
| Mongo Express       | http://localhost:8082  |
| Kafka Connect       | http://localhost:8083  |
| pgAdmin             | http://localhost:8084  |
| KafBat (Kafka UI)   | http://localhost:8085  |
| Redis Insight       | http://localhost:5540  |
| RabbitMQ Management | http://localhost:15672 |

---

## Serviços Disponíveis

| Serviço        | Host          | Porta |
| -------------- | ------------- | ----- |
| Kafka          | kafka         | 9092  |
| Kafka Connect  | kafka-connect | 8083  |
| PostgreSQL     | postgres      | 5432  |
| MongoDB        | mongo         | 27017 |
| Valkey (Redis) | valkey        | 6379  |
| RabbitMQ       | rabbitmq      | 5672  |

---

# Configuração Inicial

## Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd bigdata_docker
```

## Subir todos os containers

```bash
docker compose up -d --build
```

## Verificar containers ativos

```bash
docker ps
```

---

# Etapa Implementada - Produtor Kafka

Responsável: Aluno 1 (Lauro)

Foi implementado um produtor em Python responsável por gerar transações sintéticas continuamente e enviá-las para o Kafka.

Estrutura criada:

```text
producer/
├── producer.py
├── requirements.txt
└── Dockerfile
```

## Tópico Kafka utilizado

```text
vendas_raw
```

## Formato das mensagens

```json
{
  "id_transacao": "uuid",
  "id_cliente": 123,
  "valor_total": 350.50,
  "categoria": "Livros",
  "timestamp": "2026-06-22T20:00:00Z"
}
```

## Características dos dados

* Categorias aleatórias.
* Clientes aleatórios.
* Transações geradas continuamente.
* Aproximadamente 10% das transações possuem valor acima de R$ 500 para testes de detecção de fraude.

---

# Como validar o produtor

## Verificar logs

```bash
docker logs -f producer
```

Exemplo:

```text
Enviado -> Livros | R$ 250.00
Enviado -> Casa | R$ 3200.00
Enviado -> Esportes | R$ 180.00
```

## Verificar mensagens no Kafka

Acessar:

```text
http://localhost:8085
```

Navegar para:

```text
bigdata-kafka-cluster
→ Topics
→ vendas_raw
→ Messages
```

As mensagens devem aparecer continuamente.

---

## Aluno 2 - Flink + Valkey (Gabriel M.)

- Foi consumidos os dados do Kafka no tópicon: vendas_raw
- São filtrados apenas valores maiores que 500 e direcionados para o Valkey
- Foi criado o arquivo `transacao_jobpipeline.py` dentro da pasta 'pipeline'

---

# Orientações para os próximos integrantes

## Aluno 3 - MongoDB (Pedro Tronco)
...

## Aluno 4 - Airflow + Postgres (Felipe Bacchi)
...

---

# Comandos Úteis

## Iniciar ambiente

```bash
docker compose up -d --build
```

## Parar ambiente

```bash
docker compose down
```

## Reiniciar apenas o produtor

```bash
docker restart producer
```

## Visualizar logs do produtor

```bash
docker logs -f producer
```

## Visualizar containers ativos

```bash
docker ps
```
