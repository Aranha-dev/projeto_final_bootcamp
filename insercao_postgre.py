import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class Connectar:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        
    def conectar(self):
        keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 60,
        "keepalives_interval": 20,
        "keepalives_count": 10}

        cnx = psycopg2.connect(host=self.host, database=self.database, user=self.user,password=self.password, **keepalive_kwargs)
        return cnx
    
    def selecionar(self, query):
        cnx = self.conectar()
        cursor = cnx.cursor()
        cursor.execute
    
    def selecionar(self, query):
        cnx = self.conectar()
        cursor = cnx.cursor()
        cursor.execute(query)
        resultado = cursor.fetchall()
        return resultado
    
    def acao(self, query):
        cnx = self.conectar()
        cursor = cnx.cursor()
        cursor.execute(query)
        cursor.close()
        
        cnx.commit()
        
        return "Acao feita!"
    
    def inserir(self, table, parametros, valores):
        query = f"INSERT INTO {table} ({parametros}) VALUES ({valores})"
        self.acao(query)
    
    def inserir_multiples(self, table, parametros, valores):
        query = f"INSERT INTO {table} ({parametros}) VALUES {valores}"
        self.acao(query)
        
    def ler_csv(self, path):
        dataframe = pd.read_csv(path)
        return dataframe

    
c1 = Connectar(host="34.95.140.217", database="desafio_final", user="postgres",password="root")

# ****************************************************************************
# INSERCAO NA PRIMEIRA TABELA
# ****************************************************************************
df = c1.ler_csv('gs://bucket-projeto-final/csv/pib_agricola.csv')
df['Ano'] = pd.to_numeric(df['Ano'], errors = 'coerce')

for i in range(df.shape[0]):
    c1.inserir('pib_agricola', 'ano_pib, insumo, agropecuaria, industria, servicos, total', f'{df.Ano[i]}, {df.Insumo[i]},\
        {df.Agropecuraria[i]}, {df.Industria[i]}, {df.Servicos[i]}, {df.Total[i]}')
    
# ****************************************************************************
# INSERCAO NA SEGUNDA TABELA
# ****************************************************************************

df1 = c1.ler_csv("gs://bucket-projeto-final/csv/dados_usa_tratado.csv")
df1.drop("Unnamed: 0", axis=1, inplace=True)
arr = np.array(df1)
lista_antes = []
for i in arr:
    item = (i[0], i[1])
    lista_antes.append(item)
lista_antes = str(lista_antes)[1:-1]
c1.inserir_multiples('usa_agricultura', 'ano_usa, valores', lista_antes)

# ****************************************************************************
# INSERCAO NA TERCEIRA TABELA
# ****************************************************************************

df2 = c1.ler_csv('gs://bucket-projeto-final/csv/valor_producao(FAO).csv')
df2.drop("Unnamed: 0", axis=1, inplace=True)
arr = np.array(df2)
lista_antes = []
for i in arr:
    item = (i[0], i[1], i[2], i[3])
    lista_antes.append(item)
lista_antes = str(lista_antes)[1:-1]
c1.inserir_multiples('valor_producao','pais,item, ano_producao, valor_item', lista_antes)

#****************************************************************************
# INSERCAO NA QUARTA TABELA
#****************************************************************************
try:
    df3 = c1.ler_csv('gs://bucket-projeto-final/csv/quantidade(FAO)_final.csv')
    df3.drop("Unnamed: 0", axis=1, inplace=True)
    df3 = df3.fillna(0)

    tam = df3.shape[0]

    k = 0
    j = 500000
    
    while True:
        arr = np.array(df3)
        valores = []
        
        if j > 3000000: 
            j = 3000000 + (tam - 3000000)

        for i in arr[k:j]:
            item = (i[0], i[1], i[2], i[3], i[4], i[5])
            valores.append(item)
        valores = str(valores)[1:-1]
        # print(lista_antes)
        c1.inserir_multiples('quantidade_colheita','pais, item, elemento, ano_quantidade, unidade, valor_quantidade', valores)
        if j == tam:
            break
        j += 500000
        k += 500000
except Exception as e:
    print(e)

#****************************************************************************
# INSERCAO NA QUINTA TABELA
#****************************************************************************

lista = []
for line in pd.read_csv('gs://bucket-projeto-final/csv/exportacao_paises.csv'):
    lista.append(line)
    
tam = len(lista)

lista_values = []
for i in range(tam):
    lista_values.append(lista[i][1:-1])

for item in lista_values:
    c1.inserir_multiples('exportacao_pais','pais_exportacao,valor_exportacao,ano_exportacao',item)


#****************************************************************************
# INSERCAO NA SEXTA TABELA
#****************************************************************************


df5 = pd.read_csv("gs://bucket-projeto-final/csv/total_exportacao.csv")
df5.drop("Unnamed: 0", axis=1, inplace=True)

arr = np.array(df5)
lista_values = []
for i in arr:
    item = (i[0], i[1])
    lista_values.append(item)
lista_values = str(lista_values)[1:-1]

print(lista_values)
c1.inserir_multiples('total_exportacao','total_exportacao, ano_total', lista_values)



#****************************************************************************
# INSERCAO NA SÉTIMA TABELA
#****************************************************************************

lista = []
for line in pd.read_csv('gs://bucket-projeto-final/csv/exportacao_uf.csv'):
    lista.append(line)
    
tam = len(lista)

lista_values = []
for i in range(tam):
    lista_values.append(lista[i][1:-1])

for item in lista_values:
    print(item)
    c1.inserir_multiples('exportacao_estado','estado,valor_estado,ano_estado',item)


#****************************************************************************
# INSERCAO NA OITAVA TABELA
#****************************************************************************

lista = []
for line in pd.read_csv('gs://bucket-projeto-final/csv/exportacao_produtos.csv'):
    lista.append(line)

tam = len(lista)

lista_values = []
for i in range(tam):
    lista_values.append(lista[i][1:-1])

for item in lista_values:
    print(item)
    c1.inserir_multiples('exportacao_produto','produto, valor_produto, ano_produto',item)
    