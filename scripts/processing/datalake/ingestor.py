from __future__ import annotations
from abc import ABC, abstractmethod


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
    def operation_starter(self):
        print("persist file into GCS no matter the zone")


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