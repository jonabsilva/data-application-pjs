from __future__ import annotations
from abc import ABC, abstractmethod

from bs4 import BeautifulSoup
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


#code_process_list = ["1028260-20.2021.8.26.0007"]


class ESajDataFiles(ABC):
    """
    Each distinct product of a product family should have a base interface. All
    variants of the product must implement this interface.
    """

    @abstractmethod
    def start_scraping(self) -> str:
        pass


"""
Concrete Products are created by corresponding Concrete Factories.
"""


class ESajData(ESajDataFiles):
    def get_movimentacoes(self, code_process: str):
        datas = []
        descricoes = []
        url_call = f"{URL}{END_POINT}{PATTERN}{CD_PROCESS}{code_process}"
        page = requests.get(url_call)
        soup = BeautifulSoup(page.text, "html.parser")
        data_elements = soup.find_all('td', class_="dataMovimentacao")
        desc_elements = soup.find_all('td', class_="descricaoMovimentacao")
        for element in data_elements:
            data_text = element.get_text(strip=True)
            datas.append(data_text)
        for desc in desc_elements:
            desc_text = desc.get_text(strip=True)
            descricoes.append(desc_text.replace('\n', '').replace('\t', ''))

        #df = pd.DataFrame({'Data': datas, 'Descrição': descricoes})
        dataframe = {'data': datas, 'descricao': descricoes}    
        return dataframe
    

    # CABECALHO
    def get_cabecalho(self, code_process: str):
        url_call = f"{URL}{END_POINT}{PATTERN}{CD_PROCESS}{code_process}"
        page = requests.get(url_call)
        process_number = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="numeroProcesso"
                                                        ).text.strip()
        process_situation = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="labelSituacaoProcesso"
                                                        ).text.strip()
        process_class = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="classeProcesso"
                                                        ).text.strip()
        process_subject = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="assuntoProcesso"
                                                        ).text.strip()
        process_foro = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="foroProcesso"
                                                        ).text.strip()
        process_vara = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="varaProcesso"
                                                        ).text.strip()
        process_judge = BeautifulSoup(page.text, 
                                    "html.parser").find('span', 
                                                        id="juizProcesso"
                                                        ).text.strip()
        process_distribuition = BeautifulSoup(page.text, 
                                    "html.parser").find('div', 
                                                        id="dataHoraDistribuicaoProcesso")
        
        process_distri_date = process_distribuition.text[10]
        process_distri_time = process_distribuition.text[14:19]
        process_distri_status = process_distribuition.text[19]
        process_control = BeautifulSoup(page.text, 
                                    "html.parser").find('div', 
                                                        id="numeroControleProcesso"
                                                        ).text.strip()
        process_area = BeautifulSoup(page.text, 
                                    "html.parser").find('div', 
                                                        id="areaProcesso"
                                                        ).text.strip()
        process_value = BeautifulSoup(page.text, 
                                    "html.parser").find('div', 
                                                        id="valorAcaoProcesso"
                                                        ).text.strip()
        
        data_raw = [[process_number, process_situation, process_class]]
        #print(process_value.text.strip())#.replace('  ', ''))
        # data cleansing
        data = [[item.strip() for item in row] for row in data_raw]
        # Definindo os nomes das colunas
        columns = ["Codigo_do_Processo", "Classe", "Assunto"]
        # Criando o DataFrame
        data =  [[process_number, process_situation, process_class]]#[columns, data]
        return pd.DataFrame(data, columns=columns) #dataframe

    # scraping data and add into dataframe
    def start_scraping(self, dataframe= dict):
        if type(dataframe) == 'pandas.core.frame.DataFrame':
            # Ensure all lists are of the same length
            lengths = [len(v) for v in dataframe.values()]
            if len(set(lengths)) > 1:
                raise ValueError(
                    "All lists in the dictionary must be of the same length")

            # Convert the data dictionary to a pandas DataFrame
            df = pd.DataFrame(dataframe).to_string(index=False)
            return df
        else:
            return dataframe

    def __repr__(self):
        attributes = ', '.join(
            f"{key}={value}" for key, value in self.__dict__.items())
        return f"DinamicWithList({attributes})"


#cd_processo = ESajData()
#
#for a in code_process_list:
#    gt_cd_process = cd_processo.get_process_code(a)
#    gt = cd_processo.start_scraping(dataframe=gt_cd_process)
#
#    print(gt)
#