
import pandas as pd
import numpy as np
import codecs
from chardet.universaldetector import UniversalDetector
import psycopg2
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import folium as folium


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

colunas = ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante","Latitude","Longitude"]

df1 = df_json_automatico[colunas].copy()

df1.rename(
    columns={
        'Numero_da_Ocorrencia':'numOcorrencia',
        'Classificacao_da_Ocorrência':'classifOcorrencia',
        'Data_da_Ocorrencia':'dt_Ocorrencia',
        'Municipio':'nm_mun',
        'UF':'uf',
        'Regiao':'regiao',
        'Nome_do_Fabricante':'nm_Fabricante',
        'Latitude':'lat',
        'Longitude':'long'
        },
    inplace=True
    )

df1['lat'] = df1['lat'].astype(str).str.replace(',', '.')
df1['long'] = df1['long'].astype(str).str.replace(',', '.')

df1['lat'] = df1['lat'].replace('None', np.nan)
df1['long'] = df1['long'].replace('None', np.nan)

if df1['lat'].dtype == 'object':
    df1['lat'] = df1['lat'].str.replace(',', '.').astype(float)

if df1['long'].dtype == 'object':
    df1['long'] = df1['long'].str.replace(',', '.').astype(float)


dbname   = 'dbname'
user     = 'user'
password = 'password'
host     = 'host'
port     = 'port'

conexao = psycopg2.connect(dbname=dbname,
                        user=user,
                        password=password,
                        host=host,
                        port=port)

cursor = conexao.cursor()

cursor.execute('delete from gisdb.desenvolvimento.dataset_anac_geo')

for indice,coluna_df1 in df1.iterrows():
    cursor.execute ("""
                INSERT INTO gisdb.desenvolvimento.dataset_anac_geo (numOcorrencia,classifOcorrencia,dt_Ocorrencia,nm_mun,uf,regiao,nm_Fabricante,lat,long) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (coluna_df1["numOcorrencia"],coluna_df1["classifOcorrencia"],coluna_df1["dt_Ocorrencia"],coluna_df1["nm_mun"],coluna_df1["uf"],coluna_df1["regiao"],coluna_df1["nm_Fabricante"], coluna_df1["lat"],coluna_df1["long"])                   
                )
conexao.commit()
cursor.close()
conexao.close()

geometry = [Point(xy) for xy in zip(df1['long'], df1['lat'])]

gdf = gpd.GeoDataFrame(df1, geometry=geometry, crs="EPSG:4326") 

gdf_sem_nan = gdf.copy()

gdf_sem_nan = gdf_sem_nan[gdf_sem_nan.geometry.notna()]

for index, acidentes in gdf_sem_nan.iterrows():
    if acidentes.geometry.geom_type == "Point":
        if pd.isna(acidentes.geometry.x) or pd.isna(acidentes.geometry.y):
            gdf_sem_nan.drop(index, inplace=True)
    else:
        if pd.isna(acidentes.geometry.centroid.x) or pd.isna(acidentes.geometry.centroid.y):
            gdf_sem_nan.drop(index, inplace=True)

gdf = gdf_sem_nan.copy()


initial_location = [-15.793889, -47.882778]

acidentes_anac_map = folium.Map(
                                location=initial_location, 
                                zoom_start=10, 
                                control_scale=True, 
                                width="100%", height="100%"
                                )

folium.TileLayer('openstreetmap', attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors').add_to(acidentes_anac_map)
folium.TileLayer('stamenterrain', attr='Stamen Terrain').add_to(acidentes_anac_map)
folium.TileLayer('cartodb positron', attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://cartodb.com/attributions">CartoDB</a>').add_to(acidentes_anac_map)

AcidentesAnac_group = folium.FeatureGroup("Acidentes Anac")

for index, acidentes in gdf.iterrows():
    try:
        if acidentes.geometry.geom_type == "Point":
            longitude, latitude = acidentes.geometry.x, acidentes.geometry.y
        else:
            longitude, latitude = acidentes.geometry.centroid.x, acidentes.geometry.centroid.y

        tipo_ocorrencia = acidentes["classifOcorrencia"]
        data_ocorrencia = acidentes['dt_Ocorrencia']
        fabricante = acidentes["nm_fabricante"] if "nm_fabricante" in acidentes else "Informação Indisponível"

        popup_html = f"Acidentes Anac: {tipo_ocorrencia}<br>Data da Ocorrência: {data_ocorrencia}<br>Fabricante: {fabricante}"

        folium.Marker(
                        [latitude, longitude],
                        icon=folium.Icon(color="red", icon="plane", prefix="fa"),
                        popup=folium.Popup(html=popup_html, max_width=450),
                        tooltip=tipo_ocorrencia,
                    ).add_to(AcidentesAnac_group)
    except AttributeError as e:
        print(f"Erro ao processar linha {index}: {e}")
        print(acidentes)

AcidentesAnac_group.add_to(acidentes_anac_map)
folium.LayerControl().add_to(acidentes_anac_map)
acidentes_anac_map.save("acidentes_anac_map.html")