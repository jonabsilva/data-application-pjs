from __future__ import annotations
from abc import ABC, abstractmethod

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import requests
import io

import ssl

###############
credentials_drive_ = "/Users/jonathanbrunosilva/Documents/JJTECH/BNS/Service-Account/data-application-pjs.json"

# Autenticar na API do Google Drive
credentials_drive = service_account.Credentials.from_service_account_file(
    credentials_drive_, scopes=['https://www.googleapis.com/auth/drive.files'])

drive_service = build('drive', 'v3', credentials=credentials_drive)

# Your API key
API_KEY = "AIzaSyCoqEmfXRQqRFpoAfRLqAznczv-X1D6Vh4"

# File ID of the file you want to access
file_id = '1NYLMVpIB-f99E1TkLyb_rrM9kb9b9Xu8'
###############

# Credentials - Certificate
ssl._create_default_https_context = ssl._create_unverified_context


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
    def start_gdrive_extraction(self) -> str:
        try:
            # Download the file from Google Drive

            url = f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&key={API_KEY}'
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
        return f"The result start extraction of GDrive.\n {df}"

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

    print(f"{get_gdrive_data.start_gdrive_extraction()}")
    #print(f"{get_gdrive_data.another_useful_function_b(scraping)}", end="")
