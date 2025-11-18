"""
main.py

Proyecto Final Adquisición de Datos: Análisis de Datos F1.

Grupo 04
    - Alejandro Alcázar Mendoza
    - Ana Ling Gil González
    - Raynel Antonio García Bryan
    - Leyre Fontaneda Fernández

Archivo a ejecutar que emplea los módulos de crawler.py, jolpica.py y merge.py
"""

# ================ Imports de los módulos necesarios ================ #

from src.crawler import start_crawler
from src.jolpica import get_drivers_mapping, get_pit_stops
from src.merge import merge_API_WIKI

colors = {"default": "\033[0m", "red": "\033[31m", "green": "\033[32m", "blue": "\033[34m"}


if __name__ == "__main__":

    # ================ Web Scraping con Scrapy ================ #
    print(f"{colors['blue']}Web scraping de la página web de Wikipedia con Scrapy{colors['default']}")
    start_crawler()
    print(f"{colors['green']}Web scraping terminado{colors['default']}")

    # ================ Peticiones a la Jolpica F1 API ================ #
    print(f"{colors['blue']}Peticiones a la Jolpica F1 API{colors['default']}")
    get_drivers_mapping()
    try:
        get_pit_stops()
    except FileNotFoundError as error:
        print(f"{colors['red']}No se encontró el archivo de drivers.csv en results{colors['default']}")
    print(f"{colors['green']}Peticiones a la Jolpica F1 API terminado{colors['default']}")

    # ================ Data cleaning y merge ================ #
    print(f"{colors['blue']}Limpieza de datos y merge{colors['default']}")
    try:
        df, api_failed, wiki_failed = merge_API_WIKI()
        print("Dataframe final guardado en results/merged.csv")
        print(f"{colors['green']}Programa terminado{colors['default']}")
    except FileNotFoundError as error:
        print(f"{colors['red']}No se encontró el directorio de data{colors['default']}")


