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
# Verificar os tipos de colunas
df.printSchema()

# %%
# Renomeando Colunas
df = df.withColumnRenamed('Team', 'Selecao').withColumnRenamed('#', 'Numero').withColumnRenamed('Pos.', 'Posicao')\
.withColumnRenamed('FIFA Popular Name', 'Nome_FIFA').withColumnRenamed('Birth Date', 'Nascimento')\
.withColumnRenamed('Shirt Name', 'Nome na Camiseta').withColumnRenamed('Club', 'Time').withColumnRenamed('Height', 'Altura')\
.withColumnRenamed('Weight', 'Peso')
df.show(5)
# %%
# Verificando dados nulos

# Para arquivos pequenos -- Transformar em pandas
# df.toPandas().isna().sum()

# No Próprio Spark, arquivos grandes
for coluna in df.columns:
    print(coluna, df.filter(df[coluna].isNull()).count())
# %%
# Selecionar colunas
df.select('Selecao', 'Nome_FIFA').show(5)

df.select(col('Selecao'), col('Altura')).show(5)

df.select(df['Selecao']).show(5)
# %%
# Selecionar colunas com ALIAS
df.select(col('Selecao').alias('Seleção')).show(5)

df.select('Selecao Nome_FIFA Altura'.split()).show(5)
# %%
# Organizar Select
df.select(col('Nome_FIFA'), col('Peso'), col('Altura')).show(5)

df.show(5)