import pyreadr

dados = pyreadr.read_r(r'C:\Users\002763\Downloads\Dados_IBGE\Dados_20210304\RENDIMENTO_TRABALHO.rds') # also works for RData

# done! 
# result is a dictionary where keys are the name of objects and the values python
# objects. In the case of Rds there is only one object with None as key
df = dados[None] # extract the pandas data frame


df.head(5)