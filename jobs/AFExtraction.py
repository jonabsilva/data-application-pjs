from __future__ import annotations
from abc import ABC, abstractmethod

from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import pandas as pd
import json
import requests
import io
import os

from scripts.processing.datalake.ingestor import *

import ssl
# Credentials - Certificate
ssl._create_default_https_context = ssl._create_unverified_context

# Get current date
current_date = datetime.now()
# Get current year and month
current_year = current_date.year
current_month = current_date.month


class Helper:
    # Loading variables .env
    def get_env_variables(self, var: str) -> str:
        from dotenv import load_dotenv
        """
        Read a .env file and return its value as a string.
        Args:
            var (str): The name of the variable from .env.  
        Returns:
            str: The variable's value.
        """
        load_dotenv()
        get_env = os.getenv(var)
        
        return get_env


    # Read a JSON
    @staticmethod
    def get_gdrive_file_id(file_name: str) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_vars_jobs = os.path.abspath(file_name)
        with open(conf_vars_jobs, "r") as file:
            data = json.load(file)
            gdrive_file_id = str(list(data.keys())[0])

        return data[gdrive_file_id]


    # Read dalk json conf
    @staticmethod
    def get_dlk_vars(file_name: str, env: str, zone: str) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
            env: (str) Envirioment that is being runnig
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_dlk_vars = os.path.abspath(f"config/{env}/{file_name}")
        if zone == "project_config":
            with open(conf_dlk_vars, "r") as file:
                data = json.load(file)
                zone = str(list(data.keys())[0])
            
            return data[zone]
        
        elif zone == "gcs_bucket_landzone" or zone == "gcs_bucket_richzone":
            if "gcs_bucket_landzone" == zone:
                index = 1
            if "gcs_bucket_richzone" == zone:
                index = 2

            with open(conf_dlk_vars, "r") as file:
                data = json.load(file)
                zone = str(list(data.keys())[index])

        return data[zone]
    
    @staticmethod
    def clean_name(var: str, 
                   key: str, 
                   index: int, 
                   cia: str, 
                   year: str, 
                   month: str):
        """
        Replace variables and clean dlk file path.
        Args:
            var (str): Var that goes before the key.  
            key (str): key to get datalake parameter
            cia (str): Company name
            year (str): Curretly year
            month (str): Currenty month
        Returns:
            dict: The path cleaned
        """
        path = var[key][index].replace(
            "%cia", str(cia)).replace(
                "%YYYY", str(year)).replace(
                    "%mm",str(month).zfill(2))
        return path

    @staticmethod
    def clean_name_gdrive(var: str, 
                   key: str, 
                   cia: str, 
                   year: str, 
                   month: str,
                   path: str = None):

        if path == "gdrive":
            path_lz = var[key].replace(
                "%cia", str(cia).lower())
            return path_lz

        elif path == "gdrive_rich":
            path_rich = var[key].replace(
                "%cia",  str(cia).lower()).replace(
                    "%YYYY",  str(year)).replace(
                        "%mm",  str(month).zfill(2))
            return path_rich


class IIngestion(ABC):
    """
    The Abstract Factory interface declares a set of methods that return
    different abstract products. These products are called a family and are
    related by a high-level theme or concept. Products of one family are usually
    able to collaborate among themselves. A family of products may have several
    variants, but the products of one variant are incompatible with products of
    another.
    """
    @abstractmethod
    def scraping_esaj(self) -> ESajDataFiles:
        pass

    @abstractmethod
    def get_gdrive_data(self) -> GDriveFile:
        pass


class Extraction(IIngestion):
    """
    Concrete Factories produce a family of products that belong to a single
    variant. The factory guarantees that resulting products are compatible. Note
    that signatures of the Concrete Factory's methods return an abstract
    product, while inside the method a concrete product is instantiated.
    """

    def scraping_esaj(self) -> ESajDataFiles:
        return ESajData()

    def get_gdrive_data(self) -> GDriveFile:
        return GDriveData()


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
    def start_scraping(self) -> str:
        return "The result is e-Saj data. \n"


class GDriveFile(ABC):
    """
    Here's the the base interface of another product. All products can interact
    with each other, but proper interaction is possible only between products of
    the same concrete variant.
    """
    @abstractmethod
    def start_gdrive_extraction(self) -> None:
        """
        Product B is able to do its own thing...
        """
        pass




"""
Concrete Products are created by corresponding Concrete Factories.
"""


class GDriveData(GDriveFile):
    def start_gdrive_extraction(self, 
                                gdrive_file_id: str, 
                                credentials_drive: str, 
                                api_key: str):
        try:
            # Autenticar na API do Google Drive
            pattern = Helper()

            cred_drive = service_account.Credentials.\
                from_service_account_file(
                    pattern.get_env_variables(var=credentials_drive), 
                    scopes=['https://www.googleapis.com/auth/drive.files'])

            drive_service = build('drive', 'v3', credentials=cred_drive)

            # Download the file from Google Drive
            uri = "https://www.googleapis.com/drive/v3/files/"
            end_point = pattern.get_env_variables(var=api_key)
            url = f'{uri}{gdrive_file_id}?alt=media&key={end_point}'
            response = requests.get(url)

            if response.status_code == 200:
                # Create DataFrame with Pandas
                df = pd.read_csv(io.StringIO(response.content.decode()))

                # Print the DataFrame
                return df
            else:
                print('Failed to download file:', response.text)
        except Exception as e:
            print("An error occurred:", e)
        return f"The result start extraction of GDrive."

    """
    The variant, Product B1, is only able to work correctly with the variant,
    Product A1. Nevertheless, it accepts any instance of ESajDataFiles as an
    argument.
    """



## O CLIENT SERA CHAMDO DO CONCRETE PRODUCT
def af_extraction_client_code(factory: IIngestion, gcs_bucket: str) -> None:
    """
    The client code works with factories and products only through abstract
    types: IIngestion and AbstractProduct. This lets you pass any factory
    or product subclass to the client code without breaking it.
    """

    #scraping = factory.scraping_esaj()
    get_gdrive_data = factory.get_gdrive_data()
    config_vars = Helper()

    conf = config_vars.get_gdrive_file_id("config/conf-vars.json")
    cia = [cia for cia in conf.keys()]
    key_id = [key_id for key_id in conf.values()]


    gcs_conf = config_vars.get_dlk_vars(env="dev", 
                                             file_name="dlk-vars.json", 
                                             zone="project_config")
    
    zone = gcs_bucket
    bucket_name = gcs_conf[f"this_{gcs_bucket}"]
    gcs_zone = gcs_conf[f"this_{gcs_bucket}"]
    gcs_file = config_vars.get_dlk_vars(env="dev", 
                                             file_name="dlk-vars.json", 
                                             zone=zone)

    # Buckt and file name with YYYYmm and cia name vars replaced
    for cia, id in zip(cia, key_id):
        #gdrive path
        gdrive = config_vars.clean_name_gdrive(var=gcs_file, 
                                               key="gdrive", 
                                               cia=cia, 
                                               year=current_year, 
                                               month=current_month, 
                                               path="gdrive")
        
        gdrive_rich = config_vars.clean_name_gdrive(var=gcs_file, 
                                               key="gdrive", 
                                               cia=cia, 
                                               year=current_year, 
                                               month=current_month, 
                                               path="gdrive_rich")

        # esaj path
        cabecalho = config_vars.clean_name(var=gcs_file,
                                           key="esaj",
                                           index=0,
                                           cia=cia,
                                           year=current_year,
                                           month=current_month)
        
        movimentacoes = config_vars.clean_name(var=gcs_file,
                                           key="esaj",
                                           index=1,
                                           cia=cia,
                                           year=current_year,
                                           month=current_month)
        
        partes_do_processo = config_vars.clean_name(var=gcs_file,
                                           key="esaj",
                                           index=2,
                                           cia=cia,
                                           year=current_year,
                                           month=current_month)
        
        peticoes_diversas = config_vars.clean_name(var=gcs_file,
                                           key="esaj",
                                           index=3,
                                           cia=cia,
                                           year=current_year,
                                           month=current_month)

        # creating dataframe to add file into buckets  
        to_data_frame = get_gdrive_data.start_gdrive_extraction(
            gdrive_file_id=id, 
            credentials_drive="CREDENCIALS_DRIVE", 
            api_key="API_KEY")

        # Obtém os valores dos parâmetros
        df = pd.DataFrame(to_data_frame)

        # from simple factory
        task = IngestorOperator()

        persister = task.start(ingestor_type="persister", task_type="GDrive")
        if zone == "gcs_bucket_landzone":
            persister.operation_starter(bucket_name=bucket_name,
                                        df=df,
                                        gcs_file_name=f"{gcs_zone}/{gdrive}")
        
        if zone == "gcs_bucket_richzone":
            persister.operation_starter(bucket_name=bucket_name,
                                        df=df,
                                        gcs_file_name=f"{gcs_zone}/"
                                        f"{gdrive_rich}")


#if __name__ == "__main__":
#    """
#    The client code can work with any concrete factory class.
#    """
#    print("Client: Testing client code with the Extraction factory type:")
#    client_code(Extraction())
#
#    print("\n")