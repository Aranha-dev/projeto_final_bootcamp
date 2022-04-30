from pyspark.sql import SparkSession
import os
# ------------------------------------- PARQUET ------------------------------------------
os.environ["SPARK_HOME"] = "/usr/lib/spark spark-submit --packages gs://bucket-projeto-final/postgresql-42.3.1.jar gs://bucket-projeto-final/parquets.py"

"""[Leitura do spark]
export SPARK_CLASSPATH=/home/edudev/Documents/trabalho_final/modules/jars/sparkql-42.3.1.jar
spark-submit --driver-class-path /home/edudev/Documents/trabalho_final/modules/jars/postgresql-42.3.1.jar
spark-submit --driver-class-path gs://bucket-projeto-final/postgresql-42.3.1.jar gs://bucket-projeto-final/parquets.py

"""
spark = SparkSession.builder.appName('Postgres to Parquet').getOrCreate()  

def read_transform_parquet(table_name, path_parquet):

    url = 'jdbc:postgresql://34.95.140.217:5432/desafio_final'
    
    properties = {
    
    'user': 'postgres',
    'password': 'root',
    'driver': 'org.postgresql.Driver'
    }

    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    
    df.write.parquet(path_parquet)

    return f"Parquet add in {path_parquet}"

read_transform_parquet('pib_agricola', 'gs://bucket-projeto-final/parquet/pib_agricola')
read_transform_parquet('valor_producao', 'gs://bucket-projeto-final/parquet/valor_producao')
read_transform_parquet('quantidade_colheita', 'gs://bucket-projeto-final/parquet/quantidade_colheita')
read_transform_parquet('exportacao_pais', 'gs://bucket-projeto-final/parquet/exportacao_pais')
read_transform_parquet('usa_agricultura', 'gs://bucket-projeto-final/parquet/usa_agricultura')
read_transform_parquet('total_exportacao', 'gs://bucket-projeto-final/parquet/total_exportacao')
read_transform_parquet('exportacao_estado', 'gs://bucket-projeto-final/parquet/exportacao_estado')
read_transform_parquet('exportacao_produto', 'gs://bucket-projeto-final/parquet/exportacao_produto')
    