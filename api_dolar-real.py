from asyncore import read
from gettext import install

import requests # biblioteca para acessar a API
import json # biblioteca para ler arquivo formato json

requisicao = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL") #acessando a API

cotacoes = requisicao.json() # desserializando retorno da API

cotacoes

cotacao_dolar = cotacoes['USDBRL']["bid"] # selecionando apenas a cotação do dólar

print (cotacao_dolar)

