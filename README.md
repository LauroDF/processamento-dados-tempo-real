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

- Foram consumidos os dados do Kafka no tópico: 'vendas_raw'
- São filtrados apenas valores maiores que 500 e direcionados para o Valkey
- Foi criado o arquivo `transacao_jobpipeline.py` dentro da pasta 'pipeline'

---

## Aluno 3 - MongoDB (Pedro Tronco)

- Criado arquivo [`pipeline/datalake_jobpipeline.py`](/pipeline/datalake_jobpipeline.py) para fazer a conexão por pyflink
- Alterado [`docker-compose.yml`](docker-compose.yml) para rodar arquivo do pipeline pelo container `flink-jobmanager` no startup
- Criados Database `transacoes` e Collection `dados_brutos` no mongodb

### Para validar o data-lake:

1- Acessar: [`http://localhost:8082/db/transacoes/dados_brutos`](http://localhost:8082/db/transacoes/dados_brutos)


2- Fazer login:
  - Username: `admin`
  - Password: `pass`

3- Atualizar página e verificar novos documentos sendo criados.

### Exemplo de documento criado no mongo:
```json5
{
  _id: ObjectId('6a3ddaae01186d4028c958a4'),
  id_transacao: '060ea2ce-48fb-45c1-80b5-d62fc08c07b7',
  id_cliente: 49,
  valor_total: 258.6099853515625,
  categoria: 'Casa',
  timestamp: '2026-06-26T01:49:33.646672+00:00'
}
```

---
## Aluno 4 - Airflow + Postgres (Felipe Bacchi)

* Foi adicionado o ecossistema do **Apache Airflow** (Webserver e Scheduler) ao arquivo `docker-compose.yml`.
* Foi criada a DAG `faturamento_categoria_dag` no arquivo [`dags/faturamento_dag.py`](dags/faturamento_dag.py).
* A DAG é disparada a cada **2 minutos** (em lote) para:
  1. Ler dados brutos do MongoDB (database `transacoes`, collection `dados_brutos`).
  2. Agregar o faturamento total e quantidade de transações por categoria de produto em Python.
  3. Realizar um `upsert` na tabela relacional `faturamento_categoria` no PostgreSQL (banco `bigdata`).

### Para validar a orquestração e agregação:

1. **Acessar a Airflow UI**:
   * URL: [`http://localhost:8089`](http://localhost:8080)
   * Credenciais: Usuário `admin` / Senha `admin`
   * Ative a DAG `faturamento_categoria_dag` no painel.

2. **Verificar os dados no PostgreSQL**:
   * Abra o pgAdmin no navegador: [`http://localhost:8084`](http://localhost:8084) (ou utilize o DBeaver/outro cliente na porta externa `5434`, banco `bigdata`, usuário `admin`, senha `admin`).
   * Execute a seguinte consulta SQL:
     ```sql
     SELECT * FROM faturamento_categoria ORDER BY faturamento_total DESC;
     ```
   * Verifique se as categorias de vendas (ex.: `Eletrônicos`, `Livros`, `Roupas`, etc.) estão sendo atualizadas com o faturamento correto acumulado a partir do MongoDB.

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
