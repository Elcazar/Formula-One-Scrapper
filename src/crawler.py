"""
crawler.py
Módulo para hacer web scraping de la página web de Wikipedia de las distintas temporadas de Formula 1
    - La función principal es start_crawler
    - Guarda los datos de las carreras dentro de una carpera "data"
    - El formato de nombre para cada df es WIKI_{current_round}_{race}.csv
    - Race es de la forma {year}_{racename}
"""

# ================ Imports ================ #

import scrapy
from scrapy.crawler import CrawlerProcess
import regex as re
import os
import pandas as pd

colors = {
    "default": "\033[0m",    
    "red": "\033[31m",        
    "green": "\033[32m",     
    "blue": "\033[34m",      
}


class F1TablesSpider(scrapy.Spider):
    name = "f1_tables"

    def start_requests(self):
        urls = [
            f"https://en.wikipedia.org/wiki/{year}_Formula_One_World_Championship" for year in range(2012, 2024)
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Extraemos el año de la URL usando regex
        match = re.search(r'/(\d{4})_', response.url)
        year = match.group(1) if match else 'unknown'

        # Buscamos las tablas con clase "wikitable sortable"
        tables = response.css('table.wikitable.sortable')
        table_found = False

        if tables:
            for table in tables:
                # Extraemos la tabla que tenga "Report" en los headers
                headers = table.css('th').css('*::text').getall()
                headers = [header.strip() for header in headers if header.strip()]

                if "Report" in headers:
                    table_found = True
                    self.log(f"{colors['green']}Tabla de reports encontrada: {year}{colors['default']}")

                    # Buscamos en cada row el enlace. Necesitamos también la ronda 
                    rows = table.css('tr')
                    current_round = 0
                    for row in rows[1:-1]:  # Omitimos la fila de headers y la última fila "Source"
                        current_round += 1
                        cells = row.css('td')
                        report_cell = cells[-1]
                        report_link = report_cell.css('a::attr(href)').get()
                        if report_link:
                            report_link = response.urljoin(report_link)
                            # Seguimos el enlace
                            yield scrapy.Request(url=report_link, callback=self.parse_report, meta={'year': year, "current_round": current_round})
                        else:
                            self.log(f"{colors['red']}No se encontró enlace en la fila.{colors['default']}")
            if not table_found:
                self.log(f"{colors['red']}Tabla de reports NO encontrada: {year}{colors['default']}")
        else:
            self.log(f"{colors['red']}Tabla de reports NO encontrada: {year}{colors['default']}")

    def parse_report(self, response):
        # Primero buscamos el año y la competición en response
        race = response.url.split("/")[-1]
        year = response.meta.get('year', 'unknown')
        current_round = response.meta.get('current_round', 'unknown')

        # Buscamos las tablas, hay de 2 tipos
        sortable_tables = response.css('table.wikitable.sortable')
        basic_tables = response.css('table.wikitable')
        tables = sortable_tables + basic_tables
        table_found = False

        if tables:
            for table in tables:
                # Extraemos la tabla que tenga "Points" y "Laps" en los headers (race classification)
                headers = table.css('th').css('*::text').getall()
                headers = [header.strip() for header in headers if header.strip()]

                if ("Points" in headers or "Pts." in headers) and "Laps" in headers:
                    table_found = True

                    # Guardamos la tabla como CSV
                    output_dir = f"data/{year}"
                    os.makedirs(output_dir, exist_ok=True)
                    filename = f"{output_dir}/WIKI_{current_round}_{race}.csv"

                    rows = table.css('tr')
                    data = []

                    # Extraemos headers
                    headers = [header.css('*::text').getall() for header in rows[0].css('th')]
                    headers = [' '.join(h).strip() for h in headers]

                    # Extraemos data rows
                    for row in rows[1:-1]:    # La última fila siempre es la de source
                        cells = row.css('th, td')
                        if cells:
                            row_data = [cell.css('*::text').getall() for cell in cells]
                            row_data = [' '.join(cell).strip() for cell in row_data]
                            # En los df a partir de 2019, se añade una fila más de fastets lap
                            if len(row_data) == len(headers):    # Aseguramos que se encuentren los datos correctamente
                                data.append(row_data)

                    if not headers or not data:
                        self.log(f"{colors['red']}No se pudieron extraer datos de la tabla en {response.url}{colors['default']}")
                        return

                    # Creamos un DataFrame 
                    df = pd.DataFrame(data, columns=headers)
                    df.to_csv(filename, index=False, encoding='utf-8-sig')

                    # Para el json de output
                    yield {
                        "year": year,
                        "race": race,
                        "url": response.url,
                        "table": df.to_dict(orient="records")
                    }

                    self.log(f"{colors['blue']}Tabla de clasifiación guardada: {race}{colors['default']}")

            if not table_found:
                self.log(f"{colors['red']}Tabla de clasifiación NO encontrada: {response.url}{colors['default']}")
        else:
            self.log(f"{colors['red']}Tabla de reports NO encontrada (NO TABLES): {response.url}{colors['default']}")



def start_crawler():
    """
    Ejecuta el F1TablesSpider: 
        - Accede a la tabla de reports para cada URL desde el 2012 hasta el 2023
        - Obtiene la tabla de resultados final para cada ronda de cada año
        - Guarda las tablas como csvs en una carpeta: data/<year>
    """
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                "results/crawler_output.json": {"format": "json"},  
            },
            "LOG_LEVEL": "INFO",
        }
    )

    process.crawl(F1TablesSpider)
    process.start() 
    process.join()

