from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def kafka_mongodb_pipeline():
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
    'properties.group.id' = 'datalakeGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
    )""")
   
    t_env.execute_sql("""
    CREATE TABLE mongo_sink (    
     `id_transacao` STRING,
     `id_cliente` INT,
     `valor_total` DOUBLE,
     `categoria` STRING,
     `timestamp` STRING
    ) WITH (
        'connector' = 'mongodb',
        'uri' = 'mongodb://mongo:27017',
        'database' = 'transacoes',
        'collection' = 'dados_brutos'
    )""")

    ## gerar o insert + select para ativar o pipeline
    table_result = t_env.execute_sql("""
    INSERT INTO mongo_sink
     SELECT * FROM kafka_source
    """)
    
    table_result.wait()

if __name__ == '__main__':    
    kafka_mongodb_pipeline()