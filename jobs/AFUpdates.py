from __future__ import annotations
from abc import ABC, abstractmethod

from google.oauth2 import service_account
from googleapiclient.discovery import build
from jobs.helper import Helper
from scripts.processing.datalake.ingestor import Reader

from bs4 import BeautifulSoup
from pandasql import sqldf
import pandas as pd
import requests
import ssl

# Credentials
ssl._create_default_https_context = ssl._create_unverified_context

END_POINT = "cpopg/show.do?processo.codigo="
CD_PROCESS = "&processo.numero="
# Regex
PATTERN=r'(=.*&)'
URL = f"""https://esaj.tjsp.jus.br/"""

code_process_list = ["xxxxxx", "xxxxx"]

class UpdatedProcess(ABC):
    """
    Here's the the base interface of another product. All products can interact
    with each other, but proper interaction is possible only between products of
    the same concrete variant.
    """
    @abstractmethod
    def start_updating(self) -> None:
        """
        Product B is able to do its own thing...
        """
        pass


"""
Concrete Products are created by corresponding Concrete Factories.
"""

###### APLICAR REGRA DE LER ESAJ E O ARQUIVO DA LANDZONE, COMPARAR USANDO QUERY
###### UPLOAD O ARQUIVO PARA UMA PASTA DE FILES UPDATE
class Updates(UpdatedProcess):
    # MOVIMENTACOES
    def start_updating(self, code_process: str):# query_file: str):
        datas = []
        url_call = f"{URL}{END_POINT}{PATTERN}{CD_PROCESS}{code_process}"
        page = requests.get(url_call)
        soup = BeautifulSoup(page.text, "html.parser")
        data_elements_mv = soup.find_all('td', class_="dataMovimentacao")
        data_elements_pd = soup.find_all('td', width="140")
        for element in data_elements_mv:
            data_text_mv = element.get_text(strip=True)
            datas.append(data_text_mv)
        for desc in data_elements_pd:
            data_text_pd = desc.get_text(strip=True)
            datas.append(data_text_pd)

        # Criar uma lista de valores únicos
        lista_unica = list(set(datas))
        #df = pd.DataFrame({'Data': 
        # datas, 'Descrição': descricoes})
        scraping_dataframe = pd.DataFrame({'data_unica': lista_unica})
        print("################scraping_dataframe################")
        print(scraping_dataframe)

        reader = Reader()
        csv_dataframe = reader.operation_starter(bucket_name="db_landzone_idr_00001_pjs_dev",
                                 file_path="db_landzone_idr_00001_pjs_dev/esaj/movimentacoes/movimentacoes.csv")

        print("################csv_dataframe################")
        print(csv_dataframe)

