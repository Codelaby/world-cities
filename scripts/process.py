import csv
import os
import requests
import pycountry
import pandas as pd
from io import BytesIO
from zipfile import ZipFile

header = [
    'geonameid',
    'name',
    'asciiname',
    'alternatenames',
    'latitude',
    'longitude',
    'feature class',
    'feature code',
    'country code',
    'cc2',
    'admin1 code',
    'admin2 code',
    'admin3 code',
    'admin4 code',
    'population',
    'elevation',
    'dem',
    'timezone',
    'modification date'
]

asciiCodesURL = 'http://download.geonames.org/export/dump/admin1CodesASCII.txt'

def get_country_ascii_name(alpha_2_code):
    try:
        country = pycountry.countries.get(alpha_2=alpha_2_code)
        if country:
            return country.name
        else:
            return f"No country found for alpha-2 code: {alpha_2_code}"
    except Exception as e:
        return str(e)

def map_subcountry(row, df):
    match = df[(df['subcode'] == row['subcode']) & (df['code'] == row['subcountry'])]
    if not match.empty:
        return match['asciiname'].values[0]
    else:
        return pd.NA 

def process():    
    print("Iniciando proceso de descarga y procesamiento de datos...")
    
    # Descargar y procesar cities15000.zip
    print("Descargando cities15000.zip desde GeoNames...")
    url = "http://download.geonames.org/export/dump/cities15000.zip"
    response = requests.get(url)
    zip_path = "cities15000"
    zipfile = ZipFile(BytesIO(response.content))

    print("Extrayendo cities15000.txt...")
    zipfile.extract(zip_path + ".txt")

    # Convertir a CSV
    print("Convirtiendo a formato CSV...")
    with open(zip_path + ".txt", "r") as f:
        lines = f.readlines()
    
    with open(zip_path + ".csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(header)
        for line in lines:
            writer.writerow(line.strip().split('\t')) 

    # Crear world-cities.csv
    print("Creando archivo world-cities.csv...")
    world_cities_columns = ['name', 'country', 'subcountry', 'geonameid', 'subcode', 'timezone']
    
    # Asegurarse de que el directorio data existe
    os.makedirs('data', exist_ok=True)
    
    with open('data/world-cities.csv', 'w') as outcsv:
        writer = csv.writer(outcsv, lineterminator="\n")
        writer.writerow(world_cities_columns)
        for line in lines:
            data = line.strip().split('\t')
            country = get_country_ascii_name(data[8])
            timezone = data[17]  # El campo 17 es timezone
            writer.writerow([data[1], country, data[8], data[0], data[10], timezone])
    
    print("Procesando códigos de subpaíses...")
    header_csv = ['code', 'name', 'asciiname', 'geonameid']
    df = pd.read_csv(asciiCodesURL, header=None, delimiter="\t", names=header_csv)
    world_cities = pd.read_csv('data/world-cities.csv')
    
    print("Mapeando subpaíses...")
    df['subcode'] = df['code'].str[3:]
    df['code'] = df['code'].str[:2]
    world_cities['subcountry'] = world_cities.apply(map_subcountry, axis=1, args=(df,))
    world_cities = world_cities.drop('subcode', axis=1)
    
    print("Guardando archivo final...")
    world_cities.to_csv('data/world-cities.csv', index=False)
    
    # Limpieza
    print("Limpiando archivos temporales...")
    os.remove("cities15000.txt")
    os.remove("cities15000.csv")
    
    print("Proceso completado con éxito! Archivo generado: data/world-cities.csv")

if __name__ == '__main__':
    process()
