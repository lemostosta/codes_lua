from tkinter.messagebox import RETRY
import warnings
from numpy import true_divide
import pandas as pd 
import os
import psycopg2
from openpyxl import Workbook
import warnings
import logging
import json

# Libs para criação do ambiente spark e uso das funções de tratamento de bases de dados
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Libs para acessar a API Minha Receita
import requests
import requests.adapters

warnings.simplefilter("ignore") # Ignorar mensagens de erro

# Criação do ambiente Spark
spark = SparkSession.builder.appName("ambiente_pyspark").getOrCreate()

#Lendo arquivo .csv
df = spark.read.csv(r"C:\Users\002763\Downloads\Bases de Dados\empresas.csv", header = True, inferSchema = True)

#Mostrar 50 primeiros registros
df.show(50, truncate = False)

#Filtrando apenas empresas do AM dos segmentos de interesse
provedores_am = df \
.filter(df.uf == "AM").toPandas()
#/* .where(f.upper("razao_social").like('%TELECOM%') | f.upper("razao_social").like('%TELECOMUNICACOES%')\
#    | f.upper("razao_social").like('%LINK%')| f.upper("razao_social").like('%SERVICOS DE COMUNICACAO%')\
#    | f.upper("razao_social").like('%MULTIMIDIA%')| f.upper("razao_social").like('%INTERNET%')\
#    | f.upper("razao_social").like('%TECNOLOGIA%')| f.upper("razao_social").like('%INFORMACAO%')\
#    | f.upper("razao_social").like('%DIGITAL%') | f.upper("razao_social").like('%DIGITAIS%')\
#    | f.upper("razao_social").like('%INSTALACAO%') | f.upper("razao_social").like('%MANUTENCAO%')\
#    | f.upper("razao_social").like('%ELETRICA%') | f.upper("razao_social").like('%SUPORTE TECNICO%')\
#    | f.upper("razao_social").like('%TECNICA%') | f.upper("razao_social").like('%SCM%'))\
#    .toPandas()

# Padronizando os textos
def clean_text(full_text):
    clean_text = full_text.replace("\n", " ")
    clean_text = full_text.replace(";", " ")
    return clean_text

# Os códigos de session são para tentar manter a conexão com a API mais estável
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=3, backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

session.get('https://minhareceita.org/')

# Dado um CNPJ, faz uma requisição para a API Minha Receita. 
# Caso a requisição seja bem sucedida, retorna o conteúdo da requisição em formato json
def get_cnpj_data(cnpj):
    minha_receita_api_url = 'https://minhareceita.org/'
    r = requests.post(minha_receita_api_url, data=cnpj, timeout=None, verify=False)
    if r.status_code == 200:
        return json.loads(r.content)

# Cria visualização amigável de um objeto json
def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4, ensure_ascii=False)
    print(text)

#Teste API
cnpj_example = {'cnpj': 19131243000197}
response_example = get_cnpj_data(cnpj_example)
jprint(response_example)


# Recebe um dataframe contendo os CNPJ's e, a partir das requisições à API Minha Receita, estrutura um dataframe contendo os dados do CNPJ
def cnpj_data_to_df(df_cnpj):
    data_set = pd.DataFrame(columns=[
        'cnpj',
        'razao_social',
        'nome_fantasia',
        'atividade_principal_codigo',
        'atividade_principal_descricao',
        'situacao_cadastral',
        'capital_social',
        'porte',
        'codigo_natureza_juridica',
        'data_abertura',
        'bairro',
        'cep',
        'municipio',
        'uf',
        'ddd_telefone_1',
        'descricao_situacao_cadastral'
    ])
    count = 0
    for cnpj in df_cnpj['cnpj']:
        count = count + 1 
        cnpj_data = get_cnpj_data({'cnpj': cnpj})
        if cnpj_data != None:
            new_row = {
                'cnpj': cnpj,
                'razao_social': cnpj_data['razao_social'],
                'nome_fantasia': cnpj_data['nome_fantasia'],
                'atividade_principal_codigo': cnpj_data['cnae_fiscal'],
                'atividade_principal_descricao': clean_text(cnpj_data['cnae_fiscal_descricao']),
                'situacao_cadastral': cnpj_data['descricao_situacao_cadastral'],
                'capital_social': float(cnpj_data['capital_social']),
                'porte': cnpj_data['descricao_porte'],
                'codigo_natureza_juridica': int(cnpj_data['codigo_natureza_juridica']),
                'data_abertura': cnpj_data['data_inicio_atividade'],
                'bairro': cnpj_data['bairro'],
                'cep': cnpj_data['cep'],
                'municipio': cnpj_data['municipio'],
                'uf': cnpj_data['uf'],
                'ddd_telefone_1': cnpj_data['ddd_telefone_1'],
                'descricao_situacao_cadastral': cnpj_data['descricao_situacao_cadastral']
            }
        else:
            new_row = {
                'cnpj': cnpj,
            }
        
        data_set = data_set.append(new_row, ignore_index=True)
        print(count ,len(provedores_am), sep = " de ")
    return data_set

provedores_am['cnpj']=provedores_am['cnpj'].apply(lambda x: '{0:0>14}'.format(x))  # Incluindo zeros a esquerda e fixando 14 digitos

provedores_am = df \
.filter(df.atividade_principal_codigo == "6190601").toPandas()

dados_final = cnpj_data_to_df(provedores_am)

#Salvando o arquivo final em excel no diretório de preferência
dados_final.to_excel(r"C:\Users\002763\Downloads\Bases de Dados\base_provedores_6190601.xlsx", sheet_name='df')  
