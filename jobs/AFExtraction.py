from __future__ import annotations
from abc import ABC, abstractmethod

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import json
import requests
import io
import os

import ssl
# Credentials - Certificate
ssl._create_default_https_context = ssl._create_unverified_context


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
    def get_file_id(file_name: str) -> str:
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
            field_id = str(list(data.keys())[0])

        return data[field_id]


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

#    @abstractmethod
#    def another_useful_function_b(self, collaborator: ESajDataFiles) -> None:
#        """
#        ...but it also can collaborate with the ProductA.#
#
#        The Abstract Factory makes sure that all products it creates are of the
#        same variant and thus, compatible.
#        """
#        pass


"""
Concrete Products are created by corresponding Concrete Factories.
"""


class GDriveData(GDriveFile):
    def start_gdrive_extraction(self, file_id) -> str:
        try:
            # Autenticar na API do Google Drive
            pattern = Helper()

            credentials_drive = service_account.Credentials.\
                from_service_account_file(
                    pattern.get_env_variables(var="CREDENCIALS_DRIVE"), 
                    scopes=['https://www.googleapis.com/auth/drive.files'])
            
            drive_service = build('drive', 'v3', credentials=credentials_drive)

            # Download the file from Google Drive
            uri = "https://www.googleapis.com/drive/v3/files/"
            url = f'{uri}{file_id}?alt=media&key={pattern.get_env_variables(var="API_KEY")}'
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

#    def another_useful_function_b(self, collaborator: GoogleDriveAPIClient) -> str:
#        result = collaborator.get_data_gdrive_api()
#        return f"The result of the B1 collaborating with the ({result})"

## O CLIENT SERA CHAMDO DO CONCRETE PRODUCT
def client_code(factory: IIngestion) -> None:
    """
    The client code works with factories and products only through abstract
    types: IIngestion and AbstractProduct. This lets you pass any factory
    or product subclass to the client code without breaking it.
    """
    #scraping = factory.scraping_esaj()
    get_gdrive_data = factory.get_gdrive_data()

    file_id_var = Helper()
    for id in (list(set(file_id_var.get_file_id("config/conf-vars.json").values()))):
        print(f"{str(get_gdrive_data.start_gdrive_extraction(id))}")
    #print(f"{get_gdrive_data.another_useful_function_b(scraping)}", end="")


#if __name__ == "__main__":
#    """
#    The client code can work with any concrete factory class.
#    """
#    print("Client: Testing client code with the Extraction factory type:")
#    client_code(Extraction())
#
#    print("\n")