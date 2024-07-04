from google.oauth2 import service_account
from googleapiclient.discovery import build

import pandas as pd
import io
import requests


class GoogleDriveAPIClient:
    def __init__(self, credencials: str, api_key: str, customer_info: dict):
            """
            params:
                credencials (str): JSON path of the file from your application. 
                api_key: (str) Secret key from your application.
                customer_info (dict): The key of each file on gdrive         
            """
            self.credencials = credencials
            self.api_key = api_key
            self.customer_info = customer_info

    def get_data_gdrive_api(self, cia_key: str=None) -> str:
            """
            Get data from each field ID or file on GDrive.
            Args:
                cia_key (str): A Key from conf-vars.json.  
            Returns:
                DataFrame: dataframes with the data from GDrive.
            """

            uri = "https://www.googleapis.com/"
            # Autenticar na API do Google Drive
            credentials_drive = service_account.Credentials.\
                from_service_account_file(self.credencials, 
                                          scopes=[f'{uri}drive.files'])

            drive_service = build('drive', 'v3', 
                                  credentials=credentials_drive)

            try:
                id = self.customer_info[cia_key]
                # Download the file from Google Drive
                url = f"{uri}drive/v3/files/{id}?alt=media&key={self.api_key}"
                response = requests.get(url)

                if response.status_code == 200:
                    # Create DataFrame with Pandas
                    df = pd.read_csv(io.StringIO(response.content.decode()))

                    return df
                else:    
                    return print(f"Failed to download file: {response.text}")

            except Exception as err:
                #log
                print(f"An error occurred: {err}")
