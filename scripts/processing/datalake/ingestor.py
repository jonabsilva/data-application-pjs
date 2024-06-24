from __future__ import annotations
from abc import ABC, abstractmethod

import os
from io import BytesIO

from google.cloud import storage
from google.api_core.exceptions import Conflict
import pandas as pd
from pandasql import sqldf
import io
import certifi

os.environ['SSL_CERT_FILE'] = certifi.where()


class DataLakeIgestor:
    """
    The Product interface declares the operations that all concrete products
    must implement.
    """
    @abstractmethod
    def operation_starter(self) -> str:
        pass


"""
Concrete Products provide various implementations of the Product interface.
"""

class Reader(DataLakeIgestor):
    """
    """
    def operation_starter(self,
                          bucket_name: str,
                          file_path: str) :
        client = storage.Client()
        # Get bucket
        bucket = client.get_bucket(bucket_name)
        # Get blob (file) into the bucket
        blob = bucket.blob(file_path)
        # Download blob as bytes
        csv_bytes = blob.download_as_bytes()
        # Use io.BytesIO ro read bytes in a DataFrame pandas
        csv_file = io.BytesIO(csv_bytes)
        df = pd.read_csv(csv_file)

        return df


class Loader(DataLakeIgestor):
    def operation_starter(self,
                          bucket_name: str, 
                          df: pd.DataFrame, 
                          gcs_file_name):
        """
        Uploads a DataFrame as a CSV file to Google Cloud Storage.
        Args:
            bucket_name (str): Name of the GCS bucket.
            df (pd.DataFrame): DataFrame to be uploaded
            gcs_file_name (str): Name of the file in GCS.
        """
        # Convert DataFrame to CSV string
        csv_file = df.to_csv(index=False, encoding='utf-8-sig')
        
        # Instantiates a client
        storage_client = storage.Client()

        # Uploads the CSV file to GCS
        try:
            # Verifica se o bucket já existe8
            bucket = storage_client.get_bucket(bucket_name)
            print(f"The bucket '{bucket_name}' already exists.")
        except:# Exception as e:
            if isinstance(Conflict):
                print(f"The bucket '{bucket_name}' already exists.")
            else:
                # Salvar o DataFrame como CSV no GCS
                bucket = storage_client.bucket(bucket_name)
                bucket = storage_client.create_bucket(bucket)
                print(f"Bucket '{bucket_name}' created successfuly.")
        blob = bucket.blob(gcs_file_name)
        blob.upload_from_string(csv_file, 'text/csv')

        print(f"DataFrame uploaded to gs://{bucket_name}/{gcs_file_name}")


class Persister(DataLakeIgestor):
    def operation_starter(self,
                          query_file: str,
                          bucket_name: str, 
                          df: pd.DataFrame, 
                          gcs_file_name: str):

        df_table = df #Reader().operation_starter("db_landzone_idr_00001_pjs_dev","db_landzone_idr_00001_pjs_dev/esaj/cabecalho/cabecalho.csv") #pd.DataFrame(df)

        result = sqldf(query=query_file)
        print(result)
        buffer = BytesIO()
        result.to_parquet(buffer, engine='pyarrow')
        buffer.seek(0) # Reset buffer to the begining

        # Creating as Clietn
        storage_client = storage.Client()
        # reatrive the bucket
        bucket = storage_client.bucket(bucket_name)
        # Creating a blob
        blob = bucket.blob(gcs_file_name)
        # Buffer into GCS
        #blob.upload_from_file(buffer, content_type='application/octet-stream')
        #log
        print(f"Buffer uploaded to {gcs_file_name} in bucket {bucket_name}.")


class IngestorOperator:
    @staticmethod
    def start(ingestor_type: str, task_type: str):
        if ingestor_type == "reader":
            return Reader()
        elif ingestor_type == "loader":
            if task_type == None:
                return Loader()
            elif task_type == "eSaj":
                return Loader()
            elif task_type == "GDrive":
                return Loader()
        elif ingestor_type == "persister":
            return Persister()
        else:
            raise ValueError("Invalid vehicle type")

    def stop():...







#def get_query_string(file_name: str) -> str:
#    """
#    Read a JSON file and return its data as a dictionary.
#    Args:
#        file_name (str): The name of the JSON file.  
#    Returns:
#        dict: The data from the JSON file as a dictionary.
#    """
#    conf_vars_jobs = os.path.abspath(file_name)
#    # Open and read the file .sql
#    with open(conf_vars_jobs, 'r', encoding='utf-8') as arquivo:
#        conteudo_sql = arquivo.read()
#        # Display
#        return conteudo_sql
#
#
#if __name__ == "__main__":
#    task = IngestorOperator()
#    query_string = "scripts/processing/datalake/sql/cabecalho.sql"
#    loader = task.start(ingestor_type="persister" ,task_type="eSaj")
#    
#    file_reader = Reader()
#    dfr = file_reader.operation_starter("db_landzone_idr_00001_pjs_dev",
#                                     "db_landzone_idr_00001_pjs_dev/esaj/cabecalho/cabecalho.csv")
#    loader.operation_starter(
#        query_file= get_query_string(query_string),
#        bucket_name= "db_richzone_idr_00001_pjs_dev", 
#        df=dfr, 
#        gcs_file_name= "cabecalho/2024/06/cabecalho.parquet")
#