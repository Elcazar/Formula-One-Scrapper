"""
data_cleaning.py
Módulo auxiliar de merge.py para limpiar los datos antes de hacer el merge
La función principal es clean_dfs, que toma como argumentos el año al que pertenecen y una lista
que contiene el df de la API en índice 0 y el de la Wikipedia en índice 1.
"""
# ================ Imports ================ #

import pandas as pd
import numpy as np
import regex as re
from typing import Tuple

# ================ Función para principal para devolver los dfs procesados ================ #

def clean_dfs(year_dir, df_list) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Función para limpiar los dfs 
    Args:
        year_dir (str): contiene el año al que pertenece la carrera
        df_list (list): contiene el nombre de los csvs de la API y WIKI (índices 0 y 1)
    Returns:
        api_df (pd.DataFrame): df obtenido de la API preparado para hacer merge
        wiki_df (pd.DataFrame): df obtenido de la Wikipedia preparado para hacer merge
    """
    # Primero obtenemos el año de year_dir y los nombres de los dfs
    year = int(year_dir.split("/")[1])
    api_df_name, wiki_df_name = df_list

    # Leemos cada archivo y lo almacenamos en variables para limpiarlos
    api_df = pd.read_csv(get_basename(year_dir, api_df_name))
    wiki_df = pd.read_csv(get_basename(year_dir, wiki_df_name))

    # Aplicamos las funciones para cada uno y los devolvemos
    api_df = clean_API_df(api_df, year)
    wiki_df = clean_WIKI_df(wiki_df)

    # Modificamos nombres de las columnas de wiki para que sean iguales y eliminamos una columna de start
    if "Start" in wiki_df.columns:
        wiki_df.drop("Start", axis=1, inplace=True)
    if "Laps 1" in wiki_df.columns:
        wiki_df.rename(inplace=True, columns={"Laps 1": "Laps"})

    return api_df, wiki_df

# ================ Función para limpiar el df de la API ================ #

def clean_API_df(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Limpia los DataFrames de pit-stops para su análisis:
        - Ajusta los números de los pilotos en la columna "DriverNumber" según los cambios 
        realizados en temporadas específicas (por ejemplo, el cambio al número 1 de Max Verstappen en 2022).
        - Los cambios se especifican en un diccionario con la estructura:
        {año: [(DriverId, nuevo DriverNumber)]}, permitiendo actualizar dinámicamente los datos.

    Args:
        df (pd.DataFrame): DataFrame con los datos de pit-stops.
        year (int): Año de la temporada correspondiente al DataFrame.

    Retruns:
        df (pd.DataFrame): DataFrame limpio con los números de piloto actualizados.
    """
    # Primero creamos un diccionario con los cambios necesarios
    changes = {2022: [("max_verstappen", 1)], 2023: [("lawson", 40)]}
    # Iteramos y aplicamos
    for change_year, driver_changes in changes.items():
        if year >= change_year:
            for driver_id, new_number in driver_changes:
                # Reemplazamos DriverNumber para el DriverId correspondiente
                df.loc[df["DriverId"] == driver_id, "DriverNumber"] = new_number
    
    return df

# ================ Función para limpiar el df de la Wikipedia ================ #

def clean_WIKI_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modifica las columnas de Points y de Pos. eliminando anotaciones de wikipedia
    Convierte un tiempo en formato de texto a segundos decimales para su análisis.
        - Si el tiempo está en formato "hh:mm:ss.mmm" o "mm:ss.mmm", lo convierte a segundos decimales.
        - Si el tiempo contiene "+X:XX.X", se interpreta como tiempo adicional respecto al primer piloto 
        y se calcula en base al tiempo del primer piloto (first_time).
        - Si el tiempo incluye "+X laps" o "+X lap", devuelve NaN como marcador temporal, ya que estas 
        entradas se procesan posteriormente.
        - Si el tiempo es un texto como "Withdrew", "Engine", "Collision", etc., devuelve dicho valor 
        sin modificaciones.

    Args:
        first_time: Tiempo en segundos decimales del primer piloto, usado como referencia.
        time_str: Cadena que representa el tiempo o estado del piloto.

    Returns:
        df (pd.DataFrame): df listo para hacer merge
    """
    
    # Modificamos la columna de puntos para limpiar anotaciones y transformamos a int
    df["Points"] = df["Points"].fillna(0)
    df["Points"] = df["Points"].astype(str).str.replace(r" \d| \[ p \]$", "", regex=True)
    df["Points"] = df["Points"].apply(convert_to_int)

    # Modificamos también los datos de la columna de Position para eliminar anotaciones y pasar a int
    df["Pos."] = df["Pos."].apply(convert_position)

    # Convertimos "Time/Retired" a formato decimal
    primer_tiempo = df[(df["Pos."] == "1") |(df["Pos."] == 1)]["Time/Retired"].iloc[0]
    time_pattern = re.compile(r"^(\d{1,2})?:?(\d{1,2}):(\d{2})\.(\d{3})")
    match_first_time = time_pattern.match(primer_tiempo)

    hora = int(match_first_time.group(1)) if match_first_time.group(1) is not None else 0
    minuto = int(match_first_time.group(2)) if match_first_time.group(2) is not None else 0
    segundo = int(match_first_time.group(3)) if match_first_time.group(3) is not None else 0
    milisegundos = int(match_first_time.group(4)) if match_first_time.group(4) is not None else 0

    first_time = (hora * 3600) + (minuto * 60) + segundo + (milisegundos / 1000)
    
    # Convertir todos los tiempos a formato decimal
    df["Time/Retired"] = df["Time/Retired"].apply(lambda x: time_to_decimal(first_time, x))
    
    # Rellenamos los valores de "+1 lap", "+2 laps" con el último tiempo registrado
    last_time = df["Time/Retired"][df["Time/Retired"].notna() & 
                                    (df["Time/Retired"].apply(lambda x: isinstance(x, (int, float))))].max()
    df.loc[df["Time/Retired"].isna(), "Time/Retired"] = last_time
    
    return df

# ================ Funciones auxiliares ================ #

def time_to_decimal(first_time:float,time_str: str) -> float:
    """
    Convierte un tiempo en formato hh:mm:ss.mmm a segundos, si el tiempo está en formato +X:XX.X se le añade el tiempo del primer piloto.
        - Si el tiempo está en formato mm:ss.mmm, lo convierte a decimal.
        - Si el tiempo contiene "+X laps", devuelve NaN temporalmente.
        - Si el tiempo contiene un valor diferente, como "Withdrew", "Engine"o "Electrical", devuelve dicho valor. 
    Args:
        first_time (float)
        time_str (str)
    """
    # Verificamos si el tiempo está en el formato "hh:mm:ss.mmm"
    time_pattern = re.compile(r"^(\d{1,2})?:?(\d{1,2}):(\d{2})\.(\d{3})")
    match = time_pattern.match(time_str)

    if match:
        # Si es un formato hh:mm:ss.mmm, lo convertimos a segundos
        hours = int(match.group(1)) if match.group(1) is not None else 0
        minutes = int(match.group(2)) if match.group(2) is not None else 0
        seconds = int(match.group(3)) if match.group(3) is not None else 0
        milliseconds = int(match.group(4)) if match.group(4) is not None else 0
        
        # Convertimos horas y minutos a segundos y sumamos todo
        total_seconds = (hours * 3600) + (minutes * 60) + seconds + (milliseconds / 1000)
        return total_seconds
    
    # Si el tiempo tiene "lap", es un lap adicional, devolvemos NaN temporalmente
    elif "lap" in time_str or "laps" in time_str:
        return np.nan 
    
    # Si el tiempo contiene "+" pero no "lap", es un tiempo adicional después del primer piloto
    elif "+" in time_str:
        try:
            # Si tiene el formato "+X:XX.X" (con tiempo adicional), lo convertimos a segundos
            time_str = time_str.strip("+").strip().replace(" ","")  
            time_parts = time_str.split(":")
            if len(time_parts) == 2:
                # Formato en el que hay minutos y segundos
                minutes = int(time_parts[0])
                seconds_and_milliseconds = float(time_parts[1][:3])    #cogemos los 3 primeros decimales
                return first_time+(minutes * 60) + seconds_and_milliseconds
            elif len(time_parts) == 1:
                # El tiempo es menor de un minuto
                return first_time + float(time_parts[0][:3])
            else:
                # Caso no esperado, se sustituye el valor por NaN
                return np.nan
        except:
            return time_str    #si tiene +, pero no un número lo devolvemos como está
    
    # Si el tiempo tiene "Withdrew", "Engine", "Electrical" o cualquier otro valor, devolvemos el valor
    else:
        return time_str
    

def convert_to_int(number_str:str) -> int:
    """
    Elimina el punto al final de number_str, son de la forma \d. y convierte a int
    Args:
        number_str (str): número a convertir en int
    Returns:
        int: string transformado a int
    """
    return int(number_str.strip().replace(".",""))

def convert_position(position: str) -> str:
    """
    Convierte los valores de la columna de posición en enteros sin anotaciones, DSQ o Ret
    Args:
        position (str): posición
    Returns:
        str: posición corregida sin anotaciones
    """
    position = str(position)
    # Primero comprobamos los casos que no son strings
    if position == "DSQ" or position == "Ret":
        return position
    if re.match(r"DSQ\s\d", position):
        return "DSQ"
    # Ahora en los casos numéricos
    number_pattern = re.compile(r"(\d+)\s\d+")
    match = re.search(number_pattern, position)
    if match:
        return match.groups()[0]
    else:
        return position

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

