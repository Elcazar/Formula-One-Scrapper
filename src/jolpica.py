"""
jolpica.py
Módulo para obtener información de la Jolpica FI API
    - La función get_drivers_mapping debe ejecutarse primero para obtener el mapping entre ID y número
    - La función get_pit_stops guarda para cada año desde 2019 hasta 2024 un df a partir del 
    endpoint de pitstops un df conteniendo “DriverId”, “DriverNumber”, “NPitstops” “MedianPitStopDuration”
    - Los df se guardan dentro de la carpeta data/{year}
    - El formato de nombre para cada df es API_{current_round}_{year}
"""

# ================ Imports ================ #

import requests
from urllib.parse import urlencode, urlunsplit
import time
import numpy as np
import pandas as pd
import regex as re
import os

# ================ Excepciones propias en caso de no encontrar un año, carrera o driverId ================ #

class YearNotFoundError(Exception):
    pass

class RaceNotFoundError(Exception):
    pass

class DriversNotFoundError(Exception):
    pass

# ================ Función auxiliar para API ================ #

def buid_url(path, query_dict=None) -> str:
    """
    Construye la URL dado un endpoint y unos parámetros
    Args:
        path (str): endpoint
        query_dict (dict): diccionario con parámetros 
    Returns:
        str: URL construida usando como esquema https y base api.jolpi.ca/ergast/f1
    """
    SCHEME = "https"
    BASE = "api.jolpi.ca/ergast/f1"
    if query_dict is None:
        query_dict = {}
    query = urlencode(query_dict)
    return urlunsplit((SCHEME, BASE, path, query, ""))

# ================ Función para obtener mappind id ID - número ================ #

def get_drivers_mapping(save=True) -> pd.DataFrame:
    """
    Obtiene un mapping entre el driverID y el número asociado.
    Si no se encuentra el número, se deja como NaN (np.nan)
    Args:
        save (bool): guarda los datos en un csv dentro de la carpeta de data
    Returns:
        drivers_mapping (pd.DataFrame)
    """
    # Guardamos los datos en un diccionario auxiliar
    drivers_mapping = {}

    # Iteramos para buscar todos los ID de 100 en 100
    for i in range(12):
        # Construimos la URL para cada offset con un límite de 100
        DRIVERS_URL = buid_url("drivers", {"limit": 100, "offset": i*100})
        response = requests.get(DRIVERS_URL)

        # Comprobamos que la petición es correcta
        if response.status_code == 200:
            # Comprobamos también que existan todas las claves
            try:
                json_list = response.json()["MRData"]["DriverTable"]["Drivers"]
                # Para cada diccionario del json, buscamos el driverId y su número
                for driver_dict in json_list:
                    driver_id = driver_dict.get("driverId")
                    # Si no existe permanentNumber, su valor será un NaN
                    permanent_number = driver_dict.get("permanentNumber", np.nan)
                    drivers_mapping[driver_id] = permanent_number

            except Exception as error:
                raise DriversNotFoundError              


        # Añadimos un sleep por el rate limit
        time.sleep(0.5)

    # Pasamos a df
    drivers = pd.DataFrame.from_dict(drivers_mapping, orient="index", columns=["DriverNumber"])
    # Guardamos en un csv para poder usarlo
    if save:
        os.makedirs("results", exist_ok=True)
        drivers.to_csv("results/drivers.csv", index_label="DriverId")

    return drivers_mapping

# ================ Función para obtener dfs de todas las carreras ================ #

def get_pit_stops(years = ["2019", "2020", "2021", "2022", "2023"]):
    """
    Función principal para obtener los dfs de pitstops de la API para unos años.
    Por defecto, se toma una lista de años desde 2019 hasta 2023. La lista contiene los años en formato str.
    Args:
        years (list): Lista con los años a buscar como strings
    Raises:
        FileNotFoundError si el archivo de drivers.csv en la carpeta de data no existe
    """

    # Comprobamos si existen los datos en drivers.csv
    if os.path.exists("results/drivers.csv"):
        drivers_mapping = pd.read_csv("results/drivers.csv", index_col=["DriverId"])
        drivers_mapping["DriverNumber"] = drivers_mapping["DriverNumber"].apply(lambda n: n if pd.isna(n) else int(n))
    else:
        raise FileNotFoundError
    
    for year in years:
        try:
            get_year_dfs(year, drivers_mapping)
        except YearNotFoundError as error:
            print(f"No se encontró información para el año {year}")
    
# ================ Funciones auxiliares para obtener dfs de todas las carreras ================ #

def get_year_dfs(year: int, drivers_mapping: pd.DataFrame):
    """
    Obtiene todos los dfs asociados a un año
    Args:
        year (int): año para buscar en la API
        drivers_mapping (pd.DataFrame): df que contiene la equivalencia entre ID y número
    Raises:
        YearNotFoundError: si la API da un error para el año year en la primera petición
    """
    # Primero obtenemos el total de rondas y comprobamos que exista el año
    PITS_URL = buid_url(f"{year}/last/pitstops", {"limit": 100, "offset": 0})

    response = requests.get(PITS_URL)
    if response.status_code == 200:
        try:
            rounds = int(response.json()["MRData"]["RaceTable"]["round"])
        except Exception as error:
            raise YearNotFoundError
    else:
        # Ha ocurrido un error y no se pueden obtener los df para el año
        raise YearNotFoundError

    # Para cada ronda de cada año, generamos la URL
    for current_round in range(1, rounds+1):
        try:
            # Obtenemos el df y el nombre del archivo
            pitstops = get_year_round_df(year, current_round, drivers_mapping)
            filename = f"API_{current_round}_{year}.csv"
            # Comprobamos que exista la carpeta o la creamos
            output_dir = os.path.join("data", str(year)) 
            os.makedirs(output_dir, exist_ok=True) 
            file_path = os.path.join(output_dir, filename)

            # Guardamos el df
            pitstops.to_csv(file_path, index=False) 
        except Exception as error: 
            print(f"No se pudo obtener el df para la carrera API_{current_round}_{year}")

        # Añadimos un sleep cada 3 rondas por el rate limit
        if current_round % 3 == 0:
            time.sleep(0.25)

def get_year_round_df(year: int, current_round: int, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Obtiene el df asociado a una carrera en un año concreto
    Args:
        year (int)
        current_round (int)
        mapping (pd.DataFrame)
    Returns:
        pitstops (pd.DataFrame)
    Raises:
        RaceNotFoundError si no se encontró la carrera 
    """
    # Generamos la URL
    PITS_URL = buid_url(f"{year}/{current_round}/pitstops", {"limit": 100, "offset": 0})
    response = requests.get(PITS_URL)

    # Si la respuesta es correcta, generamos el df
    if response.status_code == 200:
        json_list = response.json()["MRData"]["RaceTable"]["Races"]
        # Comprobamos que la lista no esté vacía
        if len(json_list) > 0:
            # Accedemos a PitStops, modificamos duration, agrupamos por driverId y calculamos 
            pitstops = (
                pd.DataFrame(json_list[0]["PitStops"])
                    .assign(duration = lambda df: df["duration"].apply(lambda time: convert_to_seconds(time))) 
                    .groupby("driverId")
                    .agg(NPitstops=("driverId", "count"), MedianPitStopDuration=("duration", "median"))
                    .reset_index()
                    .rename(columns={"driverId":"DriverId"})
                )
            # Solo falta completar el df con la columna de DriverNumber aplicando el mapping
            pitstops["DriverNumber"] = pitstops["DriverId"].apply(lambda driver_id: set_driver_number(driver_id, mapping))
            return pitstops
        else:
            raise RaceNotFoundError(f"El json obtenido está vacío: {year}, {current_round}")
    else:
        raise RaceNotFoundError(f"Status code distinto de 200: {response.status_code}: {year}, {current_round}")


# ================ Funciones auxiliares para procesar los dfs ================ #

def convert_to_seconds(time: str) -> float:
    """
    Convierte un tiempo dado en MM:ss.sss a segundos
    Args:
        time (str): string de tiempo
    Returns:
        float: si el formato de tiempo es correcto
        np.nan: si el formato de tiempo es incorrecto o el tiempo es un string vacío
    """
    # Primero detectamos el caso en el que no se devuelve ningún tiempo
    if not time:
        return np.nan
    
    # Para el resto de casos, construimos el patrón
    time_pattern = re.compile(r"(?:(\d+):)?(\d{2})\.(\d{3})")
    match = re.match(time_pattern, time)

    # Devolvemos el tiempo como float si hay match, si no, np.nan
    if match:
        minutes = int(match.group(1) or 0)  
        seconds = int(match.group(2))  
        milliseconds = int(match.group(3))  
        total_seconds = minutes * 60 + seconds + milliseconds / 1000  
        return round(total_seconds, 3)
    else:
        # La última posibilidad es que sea un número entero de segundos (ss.000)
        try:
            return float(time)
        except ValueError as error:
            return np.nan

def set_driver_number(driver_id: str, mapping: pd.DataFrame) -> int:
    """ Busca en el df de mapping el DriverNumber correspondiente
    Args:
        driver_id (str)
        mapping (pd.DataFrame)
    Returns:
        int (número del driver)
        np.nan (si el id tiene np.nan como DriverNumber o no existe el id)
    """
    try:
        return int(mapping.loc[driver_id])
    except KeyError as error:
        return np.nan

# ================ Funciones de tests ================ #

def print_durations(year, current_round):
    """ Función de tests"""
    PITS_URL = buid_url(f"{year}/{current_round}/pitstops", {"limit": 100, "offset": 0})
    pits = requests.get(PITS_URL).json()["MRData"]["RaceTable"]["Races"][0]["PitStops"]
    for pit in pits:
        print(pit["duration"])

def single_race(year, current_round):
    PITS_URL = buid_url(f"{year}/{current_round}/pitstops", {"limit": 100, "offset": 0})
    pits = requests.get(PITS_URL).json()["MRData"]["RaceTable"]["Races"][0]["PitStops"]
    for pit in pits:
        print(pit)
