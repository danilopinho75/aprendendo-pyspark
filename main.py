# %%
#importar bibliotecas
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Importar window functions
from pyspark.sql.window import Window

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
# %%
# Filtrar Dataframe
df.filter('Selecao = "Brazil"').show(10)

# Melhor forma de utilizar o filter
df.filter(col('Nome na Camiseta') == "FRED").show()
# %%
# Filtrar DF com 2 condições (AND / &)
df.filter((col('Selecao') == 'Argentina') &
          (col('Altura') > 180) &
          (col('Peso') >= 85)).show()

df.filter(col('Selecao') == 'Brazil').filter(col('Numero') > 20).show()
# %%
# Filtrar DF com 2 condições ( OR / |)
df.filter((col('Nome_FIFA') == 'MESSI Lionel') |
          (col('Nome_FIFA') == 'SALVIO Eduardo') | 
          (col('Altura') == 199 )).show()
# %%
# Filtrar DF Combinando & e | ( AND e OR )
df.filter((col('Nome_FIFA') == 'MESSI Lionel') |
          (col('Nome_FIFA') == 'SALVIO Eduardo') | 
          (col('Altura') == 199 ) &
          (col('Selecao') == 'Argentina')).show()

df.filter((col('Selecao') == 'Brazil') & (col('Posicao') == 'DF') |
          (col('Altura') == 199) & (col('Selecao') == 'Belgium')).show()
# %%
# Criar novas colunas (usando função lit)
df.withColumn('WorldCup', lit(2018)).show(5)
# %%
# Cria coluna condicional (usando função substring)
df.withColumn('Sub', substring('Selecao', 1, 3)).show(5)

df = df.withColumn('Ano', substring('Nascimento', -4, 4))
df.show(5)
# %%
# Criar coluna condicional ( concat / concat_ws)
df.withColumn('Concat', concat('Selecao', col('Nome na Camiseta'))).show(5)

df.withColumn('Separador', concat_ws(' - ', 'Selecao', col('Nome na Camiseta'), col('Posicao'))).show(5)

# %%
# Alterar valor de string para inteiro
df = df.withColumn('Ano', col('Ano').cast(IntegerType()))
df.printSchema()
# %%
# Criar coluna de Dia e mês

df = df.withColumn('Dia', split(col('Nascimento'), '\.')[0])
df = df.withColumn('Mes', split(col('Nascimento'), '\.')[1])


df.show(5)
df.printSchema()

# %%
# Criar a coluna Data_Nascimento
# df.show(5)
df = df.withColumn('Data_Nascimento', concat_ws('-', col('Ano'), col('Mes'), col('Dia')))
df.show(5)
# %%
# Transformar coluna como Data
df = df.withColumn('Data_Nascimento', col('Data_Nascimento').cast(DateType()))
df.show(5)
df.printSchema()
# %%
# Deletar coluna (Drop)
df = df.drop('Nascimento')
df.show(5)
# %%
# Criar backup
df2 = df
df2.show(5)
# %%
# Window Function 1 - Número de linhas - row_number()

# Agrupar por seleção
param_row_number = Window.partitionBy('Selecao').orderBy(desc('Altura'))

df.withColumn('numero_linha', row_number().over(param_row_number)).show(50)