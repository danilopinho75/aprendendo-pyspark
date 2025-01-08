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