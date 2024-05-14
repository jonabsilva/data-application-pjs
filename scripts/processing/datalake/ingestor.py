from __future__ import annotations
from abc import ABC, abstractmethod
from io import StringIO

from google.cloud import storage
import pandas as pd
import csv


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
    def operation_starter(self) :
        print("read and file and keep it")


class Persister(DataLakeIgestor):
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
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Convert CSV string to file-like object
        csv_file = StringIO(csv_data)
        
        # Instantiates a client
        storage_client = storage.Client()

        # Gets the bucket
        bucket = storage_client.bucket(bucket_name)

        # Uploads the CSV file to GCS
        blob = bucket.blob(gcs_file_name)
        blob.upload_from_file(csv_file, content_type='text/csv')

        print(f"DataFrame uploaded to gs://{bucket_name}/{gcs_file_name}")



class Structure(DataLakeIgestor):
    def operation_starter(self):
        print("trasnform data if needed")


class IngestorOperator:
    @staticmethod
    def start(ingestor_type):
        if ingestor_type == "reader":
            return Reader()
        elif ingestor_type == "persister":
            return Persister()
        elif ingestor_type == "structure":
            return Structure()
        else:
            raise ValueError("Invalid vehicle type")

    def stop():...


#if __name__ == "__main__":
#    task = IngestorOperator()
#
#    persister = task.start("persister")
#    persister.operation_starter()