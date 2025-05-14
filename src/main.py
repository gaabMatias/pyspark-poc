import pandas as pd
from pyspark.sql import SparkSession
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
spark = SparkSession.builder \
    .appName("MeuProjeto") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

def processar_com_pandas():
    df = pd.DataFrame({
        'Nome': ['Alice', 'Bob', 'Charlie'],
        'Idade': [25, 30, 35],
        'Cidade': ['SP', 'RJ', 'BH']
    })
    print("\nResultado com pandas:")
    print(df.head())

def processar_com_spark():
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["Linguagem", "Usuários"]
    df = spark.createDataFrame(data, schema=columns)
    
    print("\nResultado com Spark:")
    df.show()
    
    df.write.parquet("data/output.parquet", mode="overwrite")

if __name__ == "__main__":
    processar_com_pandas()
    processar_com_spark()
    spark.stop()