import redis
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types

# --- 1. A NOSSA REGRA PERSONALIZADA EM PYTHON ---
class SalvarNoRedis(MapFunction):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.cliente_redis = None

    def open(self, runtime_context):
        # Este método roda 1 vez quando o operário (TaskManager) liga.
        # Ele abre a conexão com o seu container do Valkey.
        self.cliente_redis = redis.Redis(host=self.host, port=self.port, decode_responses=True)

    def map(self, linha):
        # Este método roda para CADA transação que passar no filtro (>500)
        # 'linha' contém os dados do Kafka na ordem: (id, cliente, valor, categoria, timestamp)
        
        id_transacao = str(linha[0])
        
        dados = {
            "id_transacao": id_transacao,
            "id_cliente": int(linha[1]),
            "valor_total": str(round(float(linha[2]), 2)),
            "categoria": str(linha[3]),
            "timestamp": str(linha[4])
        }

        self.cliente_redis.hset(f"fraude:{id_transacao}", mapping=dados)
        
        return f"Registro fraude:{id_transacao}"

def kafka_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)     

    # Lemos do Kafka usando o conector oficial
    t_env.execute_sql("""
    CREATE TABLE kafka_source (
     `id_transacao` STRING,
     `id_cliente` INT,
     `valor_total` FLOAT,
     `categoria` STRING,
     `timestamp` STRING
    ) WITH (
    'connector' = 'kafka',
    'topic' = 'vendas_raw',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'transacaoGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
    )""")

    # 1. Fazemos a query SQL para filtrar apenas compras grandes
    tabela_filtrada = t_env.sql_query("SELECT * FROM kafka_source WHERE valor_total > 500.0")

    # 2. Convertemos a Tabela SQL para um DataStream (Fluxo de Dados contínuo)
    fluxo_dados = t_env.to_data_stream(tabela_filtrada)

    # 3. Jogamos o fluxo para a nossa função Python (que vai disparar os dados para o Valkey)
    fluxo_dados.map(SalvarNoRedis(host='valkey', port=6379), output_type=Types.STRING())

    # 4. Damos o start no motor do Flink!
    env.execute("Pipeline_Kafka_Para_Valkey")

if __name__ == '__main__':    
    kafka_pipeline()