# IMPORTAR BIBLIOTECAS
import pandas as pd
import codecs
from chardet.universaldetector import UniversalDetector
import psycopg2
import numpy as np


pd.set_option('display.max_columns', None)
caminho_do_arquivo = r"C:\Users\phmcasimiro\Documents\Cursos_Projetos\EngenhariaDadosPy\dataset_Postgre\Origem_dados\V_OCORRENCIA_AMPLA.json"
detector = UniversalDetector()

with codecs.open(caminho_do_arquivo, 'rb') as arquivo: #rb esta lendo como binario em forma de bits no lugar de texto ou outra coisa 
    for linha in arquivo:
        detector.feed(linha)
        if detector.done:
            break
detector.close()
encoding_detectado = detector.result['encoding']

df_json_automatico = pd.read_json(caminho_do_arquivo, encoding=encoding_detectado)

# LISTAR, ENTRE TODAS AS COLUNAS DO DATAFRAME, AQUELASQUE SERÃO UTILIZADAS EM UM NOVO DATAFRAME
colunas = ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante","Latitude","Longitude"]

# CRIAR UM NOVO DATAFRAME QUE RECEBE AS COLUNAS SELECIONADAS DO DATAFRAME ORIGINAL
df1 = df_json_automatico[colunas]

# RENOMEAR AS COLUNAS APAGANDO ACENTOS E RETIRANDO ESPAÇOS
df1.rename(columns={
                    'Numero_da_Ocorrencia':'numOcorrencia',
                    'Classificacao_da_Ocorrência':'classifOcorrencia',
                    'Data_da_Ocorrencia':'dt_Ocorrencia',
                    'Municipio':'nm_mun',
                    'UF':'uf',
                    'Regiao':'regiao',
                    'Nome_do_Fabricante':'nm_Fabricante',
                    'Latitude':'lat',
                    'Longitude':'long'
                    },inplace=True
            )

# SUBSTITUIR A VÍRGULA POR PONTO
df1['lat'] = df1['lat'].astype(str).str.replace(',', '.')
df1['long'] = df1['long'].astype(str).str.replace(',', '.')

# IDENTIFICAR E SUBSTITUIR STRINGS "NONE" POR VALORES "NAN" (NOT A NUMBER)
df1['lat'] = df1['lat'].replace('None', np.nan)
df1['long'] = df1['long'].replace('None', np.nan)

# CONVERTER AS COLUNAS LAT/LONG PARA O TIPO NUMÉRICO FLOAT
# É NECESSÁRIO CONVERTER VÍRGULAS EM PONTOS ANTES DE REALIZAR ESSA OPERAÇÃO
# ABAIXO ESTÁ UMA FORMA DE FAZER A CONVERSÃO DE "," PARA "." E TAXAR COMO FLOAT
if df1['lat'].dtype == 'object':
    df1['lat'] = df1['lat'].str.replace(',', '.').astype(float)

if df1['long'].dtype == 'object':
    df1['long'] = df1['long'].str.replace(',', '.').astype(float)

# PARÂMETROS DE CONEXÃO
dbname   = 'dbname'
user     = 'user'
password = 'password'
host     = 'host'
port     = 'port' 

# CRIAR CONEXÃO
conexao = psycopg2.connect(dbname=dbname,
                        user=user,
                        password=password,
                        host=host,
                        port=port)

# CRIAR UM CURSOR PARA EXECUTAR TAREFAS NO BANCO
cursor = conexao.cursor()

# DELETAR CONTEÚDO DA TABELA ANTES DA NOVA CARGA
cursor.execute('delete from gisdb.desenvolvimento.dataset_anac')

#CARGA DE DADOS
for indice,coluna_df1 in df1.iterrows():
    cursor.execute ("""
                INSERT INTO gisdb.desenvolvimento.dataset_anac (numOcorrencia,classifOcorrencia,dt_Ocorrencia,nm_mun,uf,regiao,nm_Fabricante,lat,long) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (coluna_df1["numOcorrencia"],coluna_df1["classifOcorrencia"],coluna_df1["dt_Ocorrencia"],coluna_df1["nm_mun"],coluna_df1["uf"],coluna_df1["regiao"],coluna_df1["nm_Fabricante"], coluna_df1["lat"],coluna_df1["long"])                   
                )
conexao.commit()
cursor.close()
conexao.close()