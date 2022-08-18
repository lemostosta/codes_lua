import warnings
from numpy import true_divide
import pandas as pd 
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from openpyxl import Workbook
import warnings


warnings.simplefilter("ignore")


#Parte 1 - Via API minha receita
import json
import requests


#Ambiente Spark
spark = SparkSession.builder.appName("teste_pyspark").getOrCreate()

#Lendo arquivo .csv
df = spark.read.csv("empresas.csv", header = True, inferSchema = True)

#Show
df.show(50, truncate = False)


#Esquema
df.printSchema()


#Filtrando apenas empresas do AM de Vigilância
vigilancia_am = df \
   .filter(df.uf == "AM")\
   .where(f.upper("razao_social").like('%SEGURANCA%') | f.upper("razao_social").like('%VIGILANCIA%'))\
   .show(100, truncate = False)

vigilancia_am.show(50, truncate = False)

#Filtrando apenas empresas do AM de Insitutos
instituto_am = df \
   .filter(df.uf == "AM")\
   .where(f.upper("razao_social").like('%INSTITUTO%'))\
   .show(100, truncate = False)



   
#Filtrando apenas empresas do AM de Condominio
df\
   .filter(df.uf == 'AM')\
   .where(f.upper("razao_social").like("%CONDOMINIO%"))\
   .show(20,truncate = False)    



#Filtrando apenas empresas do RR de Segurança
df\
   .filter(df.uf == 'RR')\
   .where(f.upper("razao_social").like("%SEGURANCA"))\
   .show(100, truncate=False)

   
#Filtrando apenas empresas do AM de Condominio
df\
   .filter(df.uf == 'RR')\
   .where(f.upper("razao_social").like("%CONDOMINIO%"))\
   .show(100,truncate = False)    



seguranca_rr = df\
   .filter(df.uf == 'RR')\
   .where(f.upper("razao_social").like("%SEGURANCA"))\



#--------------------------------------------------------------------------------

def get_cnpj_data(cnpj):
    # Dado um CNPJ, faz uma requisição para a API Minha Receita. Caso a requisição seja bem sucedida, retorna o conteúdo da requisição em formato json
    minha_receita_api_url = 'https://minhareceita.org/'
    r = requests.post(minha_receita_api_url, data=cnpj, timeout=None)
    if r.status_code == 200:
        return json.loads(r.content)



def jprint(obj):
        # Cria visualização amigável de um objeto json
    text = json.dumps(obj, sort_keys=True, indent=4, ensure_ascii=False)
    print(text)



#Teste API
cnpj_example = {'cnpj': 19131243000197}
response_example = get_cnpj_data(cnpj_example)
jprint(response_example)



def clean_text(full_text):
    # Limpa um texto, retirando quebras de linha e ponto-e-vírgulas
    clean_text = full_text.replace("\n", " ")
    clean_text = full_text.replace(";", " ")
    return clean_text


def cnpj_data_to_df(df_cnpj):
    # Recebe um dataframe contendo os CNPJ's e, a partir das requisições à API Minha Receita, estrutura um dataframe contendo os dados do CNPJ
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
      
        print(count ,len(dados5), sep = " de ")
    return data_set




#teste2
dados2 = seguranca_rr.toPandas()
print(dados2)

cnpj_data_to_df(dados2)


#teste3

dados3 = df\
   .filter(df.uf == 'RR')\
   .where(f.upper("razao_social").like("%CONDOMINIO%"))\
   .toPandas()



print(dados3)


dados_final = cnpj_data_to_df(dados3)


#teste4

dados4 = df\
   .filter(df.uf == 'AM')\
   .where(f.upper("razao_social").like("%SEGURANCA%"))\
   .toPandas()


print(dados4)



#teste5

dados5 = df\
   .filter(df.uf == 'AM')\
   .where(f.upper("razao_social").like("%PESQUISA%"))\
   .toPandas()




dados5['cnpj']=dados5['cnpj'].apply(lambda x: '{0:0>14}'.format(x)) # Incluindo zeros a esquerda e fixando 14 digitos
#dados5['cnpj_2']= map(lambda x: x.zfill(10), dados5['cnpj'])


dados_final = cnpj_data_to_df(dados5)


#Salvando excel
dados_final.to_excel("Instituto AM.xlsx", sheet_name='df')  