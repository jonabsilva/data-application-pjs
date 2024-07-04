# from __future__ import annotations
# from abc import ABC, abstractmethod

# from scripts.processing.datalake.ingestor import *
# from jobs.AFEsaj import *
# from jobs.AFGdrive import *

# class IIngestion(ABC):
#     """
#     The Abstract Factory interface declares a set of methods that return
#     different abstract products. These products are called a family and are
#     related by a high-level theme or concept. Products of one family are usually
#     able to collaborate among themselves. A family of products may have several
#     variants, but the products of one variant are incompatible with products of
#     another.
#     """
#     @abstractmethod
#     def scraping_esaj(self) -> ESajDataFiles:
#         pass

#     @abstractmethod
#     def get_gdrive_data(self) -> GDriveFile:
#         pass


# class Extraction(IIngestion):
#     """
#     Concrete Factories produce a family of products that belong to a single
#     variant. The factory guarantees that resulting products are compatible. Note
#     that signatures of the Concrete Factory's methods return an abstract
#     product, while inside the method a concrete product is instantiated.
#     """

#     def scraping_esaj(self) -> ESajDataFiles:
#         return ESajData()

#     def get_gdrive_data(self) -> GDriveFile:
#         return GDriveData()