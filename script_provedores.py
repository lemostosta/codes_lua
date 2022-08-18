from tkinter.messagebox import RETRY
import warnings
from numpy import true_divide
import pandas as pd 
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from openpyxl import Workbook
import warnings
import logging
import json
import requests
import requests.adapters

warnings.simplefilter("ignore")

spark = SparkSession.builder.appName("teste_pyspark").getOrCreate()

df = spark.read.csv("empresas.csv", header = True, inferSchema = True)

provedores_am = df \
   .filter(df.uf == "AM")\
   .where(f.upper("razao_social").like('%TELECOM%') | f.upper("razao_social").like('%TELECOMUNICACOES%') | f.upper("razao_social").like('%LINK%')| f.upper("razao_social").like('%SERVICOS DE COMUNICACAO%')| f.upper("razao_social").like('%MULTIMIDIA%')| f.upper("razao_social").like('%INTERNET%') | f.upper("razao_social").like('%TECNOLOGIA%')| f.upper("razao_social").like('%INFORMACAO%')| f.upper("razao_social").like('%DIGITAL%') | f.upper("razao_social").like('%DIGITAIS%'))\
.toPandas()

def clean_text(full_text):
    clean_text = full_text.replace("\n", " ")
    clean_text = full_text.replace(";", " ")
    return clean_text

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=3, backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

session.get('https://minhareceita.org/')

def get_cnpj_data(cnpj):
    minha_receita_api_url = 'https://minhareceita.org/'
    r = requests.post(minha_receita_api_url, data=cnpj, timeout=None, verify=False)
    if r.status_code == 200:
        return json.loads(r.content)


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

provedores_am['cnpj']=provedores_am['cnpj'].apply(lambda x: '{0:0>14}'.format(x))

dados_final = cnpj_data_to_df(provedores_am)

#Salvando excel
dados_final.to_excel("Provedores_AM.xlsx", sheet_name='df')  
