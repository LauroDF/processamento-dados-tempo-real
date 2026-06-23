from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            data STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'postgres.bigdata.cdc.public.teste',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'test-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE print_sink (
            data STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT data FROM kafka_source
    """)

if __name__ == "__main__":
    main()