# Proyecto: Análisis de Datos F1

Este proyecto contiene un conjunto de herramientas organizadas en varios módulos que permiten extraer y tratar datos de carreras de Fórmula 1, preparando información de eventos de carreras para su posterior análisis, obtenidos desde múltiples fuentes.

## Autores

- Alejandro Alcázar Mendoza
- Ana Ling Gil González
- Raynel Antonio García Bryan
- Leyre Fontaneda Fernández

## Introducción

El proyecto proporciona funciones para:

- Cargar datos desde la página web de Wikipedida mediante web scraping.
- Cargar datos usando la API Jolpica FI.
- Limpiar y unir todos los datos en un unico archivo.

La organización modular permite una fácil escalabilidad y mantenimiento.

## Requisitos

- **Python:** 3.11.4
- **Pandas:** 1.5.3
- **NumPy:** 1.24.3
- **Regex:** 2.5.116
- **Scrapy:** 2.8.0
- **Urllib3:** 1.26.16


## Ejecución

Para ejecutar el el proyecto:
1. Configura el entorno e instala las dependencias.
2. Ejecuta el script principal:
   ```
   python main.py
   ```

## Estructura del Proyecto

├── data                       # Carpeta con los datos de las carreras en formato CSV.
│   ├── 2012                   # Subcarpeta con datos de 2012.
│   ├── 2013                   # Subcarpeta con datos de 2013.
│   └── ...
├── docs                       # Carpeta para la documentación adicional.
│   ├── conclusion.pdf         # Documento con las conclusiones del proyecto.
│   └── graficas.pdf           # Documento con las gráficas generadas durante el proyecto.
├── modules                    # Código fuente del proyecto.
│   ├── data_cleaning.py       # Módulo para limpieza de datos.
│   ├── crawler.py             # Módulo para obtención de datos de la página Wikipedia 
│   ├── jolpica.py             # Módulo para obtención de información de la API Jolpica FI.
│   └── merge.py               # Módulo para la unión de los dataframes.
├── results                    # Carpeta con datos en formato CSV y json.
│   ├── crawler_output.json    # Output del crawler.py en formato JSON.
│   ├── drivers.csv            # Output de jolpica.py en formato csv.
│   └── merged.csv             # Resultado final del proyecto
├── main.py                    # Punto de entrada principal del proyecto.
└── README.md                  # Documentación del proyecto.


## Descripción de los Módulos

### crawler.py

Incluye funciones para realizar web scraping de la página web de Wikipedia de las carreras en las distintas temporadas de Formula 1 de 2012 hasta 2023. 
    - La función principal es start_crawler
    - Guarda los datos de las carreras dentro de una carpera "data"
    - Guarda un fichero crawler_output.json en la carpeta de "results" generado con scrapy

### jolpica.py
Mediante las funciones de este módulo se obtiene información de la API Jolpica FI, relativa a los pit-stops de las distintas temporadas de Formula 1
    - La función get_drivers_mapping debe ejecutarse primero para obtener el mapping entre ID y número. Este mapping se guarda en la carpeta de "results"
    - La función get_pit_stops guarda para cada año desde 2019 hasta 2024 un df a partir del 
    endpoint de pitstops un df conteniendo “DriverId”, “DriverNumber”, “NPitstops” “MedianPitStopDuration” en la carpeta de "data".

### merge.py
El módulo de merge se encarga de obter un único df a partir de todos los datos anteriores. Emplea un módulo auxiliar, data_cleaning.py, para procesar primero los datos.
	- La función merge_API_WIKI se encarga de hacer el merge para cada par de dataframes de una carrera en un año concreto


### data_cleaning.py
Este módulo contiene funciones para limpiar y procesar los datos crudos de las carreras. 
    - La función principal es clean_dfs, que se emplea en merge.py para limpiar los datos antes de hacer el merge. 
    - La función clean_API_df limpia el DataFrame obtenido de la API. Actualiza los números de los pilotos según un diccionario de cambios especificado por año.
    - La función clean_WIKI_df limpia el DataFrame obtenido de Wikipedia. Convierte los tiempos de los pilotos en formato "hh:mm:ss.mmm" o "+X:XX.X" a segundos decimales para un análisis más sencillo, y maneja valores especiales como "+ Laps" o "Collision". También maneja casos especiales como los puntos de vuelta rápida en la columna Points. Además, se eliminan anotaciones de Wikipedia en las columnas de Points y Positions.
    - La función time_to_decimal convierte un tiempo en formato de texto a segundos decimales. Si el tiempo es un valor relativo (por ejemplo, "+X:XX.X"), lo ajusta con respecto al primer piloto, y maneja casos como el retiro.

### main.py
	- Script para ejecutar

### Última actualización: Diciembre 2024
