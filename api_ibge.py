from email.header import Header
from pickle import FALSE
import requests
import pandas as pd

url = 'https://apisidra.ibge.gov.br/values/t/1736/n1/all/v/all/p/all/d/v44%202,v68%202,v2289%2013,v2290%202,v2291%202,v2292%202?formato=json'

data = requests.get(url).json()

df = pd.DataFrame(data)

df.head(5)


