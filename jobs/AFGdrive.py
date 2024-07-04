from __future__ import annotations
from abc import ABC, abstractmethod

from google.oauth2 import service_account
from googleapiclient.discovery import build
from jobs.helper import Helper

import pandas as pd
import requests
import io

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
                                customer_info: str, 
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
            url = f'{uri}{customer_info}?alt=media&key={end_point}'
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
