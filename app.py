import requests 
import pandas as pd
import pandasql as sql

'''
* Objetivo do script é consumir dados de uma API, transformá-los e persisti-los em um arquivo
* O arquivo deve possuir as seguintes informações:
1 - Identificadores do usuário
2 - Ultima data de adição ao carrinho de cada usuário
3 - Categoria onde houve mais adições ao carrinho por usuário
'''

# armazenando os endpoints da API
url_products = 'https://fakestoreapi.com/products'
url_carts = 'https://fakestoreapi.com/carts'
url_users = 'https://fakestoreapi.com/users'

# atribuindo o json disponível nos endpoints à variáveis
response_products = requests.get(url_products)
response_carts = requests.get(url_carts)
response_users = requests.get(url_users)

# Normalizando e transformando os jsons recebidos em dataframes quando disponíveis nas urls

## DataFrame com os dados do endpoint products
if response_products.status_code == 200:
    df_products = pd.json_normalize(response_products.json())

## DataFrame com os dados do endpoint carts
### Quebrei esse endpoint em 2 dataframes, um com as informações de carrinhos e outro para funcionar como uma tabela de controle criado a partir da chave 'products'
if response_carts.status_code == 200:
    data_carts = response_carts.json()
    df_carts = pd.json_normalize(data_carts) 
    df_carts = df_carts.drop('products',axis=1) #Eliminando a chave products pois dará origem a um dataframe único de controle
    df_control_carts_products = pd.json_normalize(data_carts,'products',['id','userId']) #dataframe de controle originado da chave 'products' 

## DataFrame com os dados do endpoint users  
if response_users.status_code == 200:
    df_users = pd.json_normalize(response_users.json())
    

#Optei por usar o SQL via querys por ter mais familiaridade com a sintáxe

#Query que filtra as informações relevantes dos usuários
query_users = '''
SELECT 
  DFU.id as UserID
, DFU.username UserName
, UPPER(DFU.`name.lastname`)||', '||UPPER(DFU.`name.firstname`) as Name
, DFU.`address.number`||' '||UPPER(DFU.`address.street`)||' '||UPPER(DFU.`address.city`)||', '||DFU.`address.zipcode` as Address
, strftime('%Y-%m-%d',DFC.date) as Date
FROM df_users DFU 
INNER JOIN df_carts DFC ON DFU.id = DFC.userId
'''

#Execução da query e atribuição do resultado em um novo DataFrame
df_users_final = sql.sqldf(query_users)


#Query que organiza os dados do DataFrame de controle
query_products = '''
 SELECT 
  dfp.category
 ,dfcp.userid
 , sum(dfcp.quantity) as Sum
 FROM df_control_carts_products DFCP
 INNER JOIN df_products dfp  on dfp.id = dfcp.productid
 group by dfp.category,dfcp.userid 
 order by dfcp.userid
 '''

#Execução da query e atribuição do resultado em um novo DataFrame
df_products_final = sql.sqldf(query_products)

#Query que faz o principal JOIN com os DataFrames gerados para montar nosso arquivo final
final_query_full = '''
SELECT DISTINCT
 DFU.UserID
,DFU.UserName
,DFU.Name
,DFU.Address
,MAX(DFU.Date) AS [Last addition to cart]
,DFP.Category AS [Most added category]
,MAX(DFP.Sum) AS [Most added item]
FROM df_users_final DFU
INNER JOIN df_products_final DFP ON DFU.UserID = DFP.UserID
GROUP BY
 DFU.UserID
,DFU.UserName
,DFU.Name
,DFU.Address
'''

#Execução da query e atribuição do resultado em um novo DataFrame
final_df_full = sql.sqldf(final_query_full)


#Query que faz uma versão do arquivo ocultando dados sensíveis 
final_query_hide = '''
SELECT DISTINCT
 DFU.UserID
,DFU.UserName
,MAX(DFU.Date) AS [Last addition to cart]
,DFP.Category AS [Most added category]
,MAX(DFP.Sum) AS [Most added item]
FROM df_users_final DFU
INNER JOIN df_products_final DFP ON DFU.UserID = DFP.UserID
GROUP BY
 DFU.UserID
,DFU.UserName
'''

#Execução da query e atribuição do resultado em um novo DataFrame
final_df_hide = sql.sqldf(final_query_hide)

#Aqui optei por gerar 2 arquivos, o primeiro com as informações mais relevantes para análises e um segundo eliminando dados sensiveis do usuário
final_df_full.to_csv('resultado_completo.csv', index = False)
final_df_hide.to_csv('resultado.csv',index = False)
