# %%
#importar bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# %%
# Iniciar Sessão do Spark

spark = (
    SparkSession.builder \
    .master('local') \
    .appName('Pyspark_01') \
    .getOrCreate()
)

# %%
# Ler arquivo
df = spark.read.csv('dataset/wc2018-players.csv', header=True, inferSchema=True)
df.show(5)
# %%
