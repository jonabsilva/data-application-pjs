from __future__ import annotations
from abc import ABC, abstractmethod
from jobs.AFExtraction import *


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
        #return "Result of the ProductExtraction {client_code(Extraction())}"
        return {af_extraction_client_code(Extraction())}


class ProductReporting(Product):
    def operation(self) -> str:
        return "{Result of the ProductReporting}"


class ProductProcessing(Product):
    def operation(self) -> str:
        return "{Result of the ProductProcessing}"