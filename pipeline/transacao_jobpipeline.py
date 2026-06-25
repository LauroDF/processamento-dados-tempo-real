import redis
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types

class SalvarNoRedis(MapFunction):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.cliente_redis = None

    def open(self, runtime_context):
        self.redis_client = redis.Redis(host=self.host, port=self.port, decode_responses=True)

    def map(self, linha):
        id_transacao = str(linha[0])
        
        dados = {
            "id_transacao": id_transacao,
            "id_cliente": int(linha[1]),
            "valor_total": str(round(float(linha[2]), 2)),
            "categoria": str(linha[3]),
            "timestamp": str(linha[4])
        }

        self.redis_client.hset(f"fraude:{id_transacao}", mapping=dados)
        return f"Registro fraude:{id_transacao}"

def kafka_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)     

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

    filtered_table = t_env.sql_query("SELECT * FROM kafka_source WHERE valor_total > 500.0")
    data_stream = t_env.to_data_stream(filtered_table)
    data_stream.map(SalvarNoRedis(host='valkey', port=6379), output_type=Types.STRING())
    env.execute("Pipeline_Kafka_for_Valkey")

if __name__ == '__main__':    
    kafka_pipeline()