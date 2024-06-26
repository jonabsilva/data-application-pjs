from __future__ import annotations
from abc import ABC, abstractmethod
from jobs.AFExtraction import Extraction, af_extraction_client_code
from jobs.AFPersister import af_persistent_client_code


class Product(ABC):
    """
    The Product interface declares the operations that all concrete products
    must implement.
    """

    @abstractmethod
    def operation(self) -> str:
        pass


"""
Concrete Products provide various implementations of the Product interface.
"""


class ProductExtraction(Product):
    def operation(self) -> str:
        return print("ProductExtraction ready") #af_extraction_client_code(factory=Extraction(), 
                                          #gcs_bucket="gcs_bucket_landzone", 
                                          #extract_type="esaj")


class ProductReporting(Product):
    def operation(self) -> str:
        return "{Result of the ProductReporting}"


class ProductProcessing(Product):
    def operation(self) -> str:
        return af_persistent_client_code(factory=Extraction(), 
                                          gcs_bucket="db_richzone_idr_00001_pjs_dev", 
                                          extract_type="esaj")