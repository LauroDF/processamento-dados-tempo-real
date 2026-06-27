import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pymongo
import psycopg2

def calcular_faturamento():
    logging.info("Iniciando a consolidação de faturamento por categoria...")
    
    # 1. Conectar ao MongoDB
    try:
        mongo_client = pymongo.MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
        db = mongo_client["transacoes"]
        collection = db["dados_brutos"]
        
        # Obter todos os documentos
        transacoes = list(collection.find())
        logging.info(f"Foram lidas {len(transacoes)} transações brutas do MongoDB.")
    except Exception as e:
        logging.error(f"Erro ao conectar ou ler do MongoDB: {e}")
        raise e

    # 2. Processar / Agregar os dados por categoria
    faturamento_por_categoria = {}
    for t in transacoes:
        cat = t.get("categoria")
        try:
            # Garante conversão segura para float
            valor = float(t.get("valor_total", 0))
        except (ValueError, TypeError):
            valor = 0.0
            
        if cat:
            if cat not in faturamento_por_categoria:
                faturamento_por_categoria[cat] = {"total": 0.0, "quantidade": 0}
            faturamento_por_categoria[cat]["total"] += valor
            faturamento_por_categoria[cat]["quantidade"] += 1

    logging.info(f"Agregação por categoria concluída. Categorias encontradas: {list(faturamento_por_categoria.keys())}")

    # 3. Conectar ao PostgreSQL
    try:
        pg_conn = psycopg2.connect(
            host="postgres",
            database="bigdata",
            user="admin",
            password="admin",
            port=5432,
            connect_timeout=5
        )
        cursor = pg_conn.cursor()
    except Exception as e:
        logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
        raise e

    # 4. Criar a tabela se não existir
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS faturamento_categoria (
                categoria VARCHAR(100) PRIMARY KEY,
                faturamento_total NUMERIC(12, 2) NOT NULL,
                quantidade_vendas INT NOT NULL,
                ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        pg_conn.commit()
        logging.info("Tabela 'faturamento_categoria' verificada/criada com sucesso no PostgreSQL.")
    except Exception as e:
        logging.error(f"Erro ao criar tabela no PostgreSQL: {e}")
        pg_conn.rollback()
        raise e

    # 5. Inserir ou atualizar os dados agregados (Upsert)
    try:
        for cat, dados in faturamento_por_categoria.items():
            cursor.execute("""
                INSERT INTO faturamento_categoria (categoria, faturamento_total, quantidade_vendas, ultima_atualizacao)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (categoria)
                DO UPDATE SET 
                    faturamento_total = EXCLUDED.faturamento_total,
                    quantidade_vendas = EXCLUDED.quantidade_vendas,
                    ultima_atualizacao = NOW();
            """, (cat, round(dados["total"], 2), dados["quantidade"]))
        
        pg_conn.commit()
        logging.info("Dados agregados de faturamento persistidos no PostgreSQL com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao inserir/atualizar dados no PostgreSQL: {e}")
        pg_conn.rollback()
        raise e
    finally:
        cursor.close()
        pg_conn.close()
        mongo_client.close()
        
    logging.info("Processo concluído com sucesso!")

default_args = {
    'owner': 'felipe_bacchi',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'faturamento_categoria_dag',
    default_args=default_args,
    description='Consolida o faturamento total por categoria a partir do MongoDB no PostgreSQL',
    schedule_interval='*/2 * * * *',  # Executa a cada 2 minutos
    catchup=False,
) as dag:

    task_consolidar = PythonOperator(
        task_id='consolidar_faturamento',
        python_callable=calcular_faturamento,
    )
