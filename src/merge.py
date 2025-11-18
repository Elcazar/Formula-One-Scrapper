"""
merge.py
Módulo para hacer el merge de los Dataframes obtenidos mediante los módulos de crawler y jolpica.
La función principal es merge_API_WIKI, que busca dentro de cada año de data los dataframes a juntar.
Este módulo emplea un módulo auxiliar, data_cleaning, para procesar los dfs antes.
"""

# ================ Imports ================ #

import pandas as pd
import os
import numpy as np
from typing import Tuple, List

# ================ Import del módulo para limpiar los datos ================ #

from src.data_cleaning import clean_dfs

# ================ Función pricipal del módulo ================ #

def merge_API_WIKI() -> Tuple[pd.DataFrame, List, List]:
    """
    Función principal de merge.py. Busca en el directorio de data las carpetas de todos los años. 
    Para cada año, hace el merge del df de la API y la Wikipedia. Si alguno no exite, se añade su
    pareja a una lista de failed. Concatena todos los dataframes y se guardan en results/merged.csv

    Returns:
        combined_df (pd.DataFrame): df final con todas las carreras
        all_api_failed (list): lista con los archivos de la API que no se han podido usar
        all_wiki_failed (list): lista con los archivos de la Wikipedia que no se han podido usar
    """
    # Tenemos que buscar dentro de la carpeta de data todos los años disponibles
    BASE = "data"
    try:
        year_dirs = [f"{BASE}/{year_dir}" for year_dir in os.listdir(BASE)]
    except FileNotFoundError as error:
        raise FileNotFoundError
    
    # Analizamos los archivos de cada año 
    all_merged = list()
    all_api_failed = list()
    all_wiki_failed = list()

    # Obtenemos el df merged y los dfs que no se han podido usar
    for year_dir in year_dirs:
        merged_dfs, api_failed, wiki_failed = merge_year_files(year_dir)
        all_merged.extend(merged_dfs)
        all_api_failed.extend(api_failed)
        all_wiki_failed.extend(wiki_failed)

    # Creamos el df combinado y modificamos columnas o nombres para mejorar la presentación
    combined_df = pd.concat(all_merged, ignore_index=True)
    combined_df.drop("No.", axis=1, inplace=True)
    combined_df.rename(inplace=True, columns={"Pos.": "Position"})
    combined_df["Laps"] = combined_df["Laps"].astype(np.int8)

    combined_df.to_csv("results/merged.csv", index=False)

    return combined_df, all_api_failed, all_wiki_failed


# ================ Función auxiliar para hacer merge en un año concreto ================ #

def merge_year_files(year_dir: str) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
    """
    Crea un df haciendo un merge para cada carrera con el df de la API y el df de la Wikipedia.
    Si no se encuentra alguno de los 2, se añade a una lista de dfs no usados (failed)
    Args:
        year_dir (str): ruta del año en el que se encuentran los dfs
    Returns:
        merged_dfs (list): lista con todos los dfs unidos
        api_failed (list): lista con los nombres de los ficheros no usados de la API
        wiki_failes (list): lista con los nombres de los ficheros no usados de la Wikipedia
    """
    # Una vez localizados los directorios, tomamos API y WIKI de cada año
    api_files = sorted([data_file for data_file in os.listdir(year_dir) if data_file.startswith("API")], key=lambda x: int(x.split("_")[1]))
    wiki_files = [data_file for data_file in os.listdir(year_dir) if data_file.startswith("WIKI")]

    # Vamos a organizarlos por un diccionario con clave: número de carrera, valor: lista con los dfs
    year_dict = {get_race_number(api_file): [api_file] for api_file in api_files}

    # Además, llevamos en una lista los dfs que no se han podido unir
    wiki_failed = []
    api_failed = []
    
    # Ahora añadimos a cada número su fichero de WIKI
    add_wiki_to_dict(wiki_files, year_dict, wiki_failed)

    # Después, para cada clave, comprobamos si existen los 2 ficheros y los procesamos
    merged_dfs = process_year_dict(year_dir, year_dict, api_failed)
    return merged_dfs, api_failed, wiki_failed

# ================ Función auxiliar para procesar el diccionario de un año ================ #

def process_year_dict(year_dir: str, year_dict: dict, api_failed:list):
    """
    Procesa los dfs del año
    Args:
        year_dir (str): directorio del año a procesar
        year_dict (dict): diccionario que contiene los dfs de la API y la Wikipedia
        api_failed (list): lista para añadir los dfs que no se han podido procesar
    """
    merged_dfs = []
    season = int(year_dir.split("/")[1])
    # Para cada clave, comprobamos si existen los 2 ficheros y los procesamos
    for race_number, df_list in year_dict.items():
        # Comprobamos primero que la lista tenga 2 archivos. Si no, solo tiene la API
        if len(df_list) == 1:
            api_failed.append(df_list[0])
        # Si tenemos 2 dfs, limpiamos y hacemos el merge
        else:
            # Si no ocurre, podemos procesar y hacer merge de los dfs
            api_df, wiki_df = clean_dfs(year_dir, df_list)
            merged_df = api_df.merge(right=wiki_df, left_on="DriverNumber", right_on="No.", how="inner")
            
            # Añadimos las columnas de Season y RaceNumber
            merged_df["Season"] = season  
            merged_df["RaceNumber"] = race_number  

            # Añadimos el df a la lista
            merged_dfs.append(merged_df)

    return merged_dfs

# ================ Funciones auxiliares para ordenar los dfs en cada año ================ #

def get_race_number(filename: str) -> str:
    """
    Retorna el número de la carrera de un fichero del tipo TYPE_RACENUMBER_
    Args:
        filename (str): nombre del archivo
    Returns:
        str: número de la carrera
    """
    return filename.split("_")[1]

def add_wiki_to_dict(wiki_files: list, year_dict: dict, wiki_failed:list):
    """
    Añade al diccionario de ficheros el archivo de WIKI. Si no existe, se añade a una lista de fallidos
    Args:
        wiki_files (list): lista de archivos de wikipedia
        year_dict (dict): diccionario con los archivos de la API
        wiki_failed (list): lista que almacena los archivos de wikipedia sin pareja de API
    """
    # Ahora añadimos a cada número su fichero de WIKI
    for wiki_file in wiki_files:
        # Tenemos que comprobar que exista la clave
        race_number = get_race_number(wiki_file)
        try:
            year_dict[race_number].append(wiki_file)
        except KeyError as error:
            wiki_failed.append(wiki_file)

def get_basename(BASE: str, filename: str) -> str:
    """
    Devuelve la ruta del archivo con la base dada
    Args:
        BASE (str): ruta base
        filename (str): nombre del archivo
    Returns:
        str: ruta completa
    """
    return f"{BASE}/{filename}"

# ================ Funciones auxiliares para limpiar los dfs de la API y la WIKI en data_cleaning.py ================ #
