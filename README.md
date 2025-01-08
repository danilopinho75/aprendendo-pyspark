# Aprender Pyspark

Este repositório contém um tutorial introdutório sobre o uso do Apache Spark com a biblioteca PySpark. O objetivo é demonstrar como realizar operações básicas de manipulação e análise de dados com PySpark, utilizando um dataset de jogadores da Copa do Mundo de 2018.

## Requisitos

Antes de executar o código, certifique-se de ter:

- Python 3.8 ou superior instalado.
- Dataset `wc2018-players.csv` na pasta `dataset/`.
- Pacotes adicionais especificados no arquivo `requirements.txt`.

Instale todos os pacotes executando:
```bash
pip install -r requirements.txt
```

## Configuração

1. Clone este repositório:
   ```bash
   git clone https://github.com/danilopinho75/aprendendo-pyspark.git
   ```
2. Navegue até o diretório:
   ```bash
   cd aprendendo-pyspark
   ```
3. Certifique-se de que o dataset está no caminho correto (`dataset/wc2018-players.csv`).

## Tópicos Abordados

### 1. Iniciar uma Sessão do Spark
Configuração da SparkSession:
```python
spark = (
    SparkSession.builder \
    .master('local') \
    .appName('Pyspark_01') \
    .getOrCreate()
)
```

### 2. Leitura de Arquivo CSV
Carregar o dataset com as opções `header` e `inferSchema` habilitadas:
```python
df = spark.read.csv('dataset/wc2018-players.csv', header=True, inferSchema=True)
```

### 3. Manipulação de Colunas
- Renomear colunas:
```python
df = df.withColumnRenamed('Team', 'Selecao')
```
- Criar novas colunas:
```python
df = df.withColumn('WorldCup', lit(2018))
```

### 4. Filtragem de Dados
Exemplo de filtro com condições:
```python
df.filter((col('Selecao') == 'Brazil') & (col('Numero') > 20)).show()
```

### 5. Conversão de Tipos e Formatação
- Alterar tipo de dados:
```python
df = df.withColumn('Ano', col('Ano').cast(IntegerType()))
```
- Transformar string em data:
```python
df = df.withColumn('Data_Nascimento', col('Data_Nascimento').cast(DateType()))
```

### 6. Análise de Dados
Exibição e seleção de colunas:
```python
df.select('Selecao', 'Nome_FIFA').show(5)
```

## Executar o Script

Para executar o script, abra seu terminal e execute:
```bash
python main.py
```

Certifique-se de que todos os requisitos estão instalados e configurados corretamente.

## Dataset

O dataset `wc2018-players.csv` contém informações sobre jogadores que participaram da Copa do Mundo de 2018, incluindo:
- Nome
- Data de nascimento
- Seleção
- Altura
- Peso
- Clube

