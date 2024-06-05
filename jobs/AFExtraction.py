from __future__ import annotations
from abc import ABC, abstractmethod

from datetime import datetime
import pandas as pd

from scripts.processing.datalake.ingestor import *
from jobs.AFEsaj import *
from jobs.AFGdrive import *
from jobs.helper import Helper

import ssl
# Credentials - Certificate
ssl._create_default_https_context = ssl._create_unverified_context

# Get current date
current_date = datetime.now()
# Get current year and month
current_year = current_date.year
current_month = current_date.month

code_process_list = ["1028260-20.2021.8.26.0007", "1008436-85.2024.8.26.0002"]


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


## O CLIENT SERA CHAMDO DO CONCRETE PRODUCT
def af_extraction_client_code(factory: IIngestion, 
                              gcs_bucket: str,
                              extract_type: str = None) -> None:
    """
    The client code works with factories and products only through abstract
    types: IIngestion and AbstractProduct. This lets you pass any factory
    or product subclass to the client code without breaking it.
    """
    # from simple factory
    task = IngestorOperator()

    get_gdrive_data = factory.get_gdrive_data()
    config_vars = Helper()

    conf = config_vars.get_gdrive_file_id("config/conf-vars.json")
    cia = [cia for cia in conf.keys()]
    key_id = [key_id for key_id in conf.values()]

    gcs_conf = config_vars.get_dlk_vars(env="dev", 
                                             file_name="dlk-vars.json", 
                                             zone="project_config")

    zone = gcs_bucket
    bucket_name = gcs_conf[f"this_{gcs_bucket}"]
    gcs_zone = gcs_conf[f"this_{gcs_bucket}"]
    gcs_file = config_vars.get_dlk_vars(env="dev", 
                                             file_name="dlk-vars.json", 
                                             zone=zone)

    cd_cabecalho = ESajData()
    for process in code_process_list:
        df_cd_cabecalho = cd_cabecalho.get_cabecalho(process)
        df_reader = cd_cabecalho.start_scraping(dataframe=df_cd_cabecalho)
        # Buckt and file name with YYYYmm and cia name vars replaced
        for cia, id in zip(cia, key_id):
            # esaj path
            cabecalho = config_vars.clean_name(var=gcs_file,
                                            key="esaj",
                                            index=0,
                                            cia=cia,
                                            year=current_year,
                                            month=current_month)

        df_cabecalho = pd.DataFrame(df_reader)
        if extract_type == "esaj":
            task_type = "eSaj"
            persister = task.start(ingestor_type="persister",
                                task_type=task_type)
            if zone == "gcs_bucket_landzone":
                persister.operation_starter(bucket_name=bucket_name,
                                            df=df_cabecalho,
                                            gcs_file_name=f"{gcs_zone}/{cabecalho}")
            #if zone == "gcs_bucket_richzone":
            #    persister.operation_starter(bucket_name=bucket_name,
            #                                df=df_esaj,
            #                                gcs_file_name=f"{gcs_zone}/"
            #                                f"{cabecalho}")

    cd_movimentacao = ESajData()
    for process in code_process_list:
        df_cd_movimentacao = cd_movimentacao.get_movimentacoes(process)
        df_reader = cd_movimentacao.start_scraping(dataframe=df_cd_movimentacao)
        # Buckt and file name with YYYYmm and cia name vars replaced
        for cia, id in zip(cia, key_id):
            # esaj path
            movimentacoes = config_vars.clean_name(var=gcs_file,
                                            key="esaj",
                                            index=1,
                                            cia=cia,
                                            year=current_year,
                                            month=current_month)

        df_movimentacao = pd.DataFrame(df_reader)

        if extract_type == "esaj":
            task_type = "eSaj"
            persister = task.start(ingestor_type="persister",
                                task_type=task_type)
            if zone == "gcs_bucket_landzone":
                persister.operation_starter(bucket_name=bucket_name,
                                            df=df_movimentacao,
                                            gcs_file_name=f"{gcs_zone}/{movimentacoes}")
            #if zone == "gcs_bucket_richzone":
            #    persister.operation_starter(bucket_name=bucket_name,
            #                                df=df_esaj,
            #                                gcs_file_name=f"{gcs_zone}/"
            #                                f"{cabecalho}")

#
#           partes_do_processo = config_vars.clean_name(var=gcs_file,
#                                           key="esaj",
#                                           index=2,
#                                           cia=cia,
#                                           year=current_year,
#                                           month=current_month)
#
#           peticoes_diversas = config_vars.clean_name(var=gcs_file,
#                                           key="esaj",
#                                           index=3,
#                                           cia=cia,
#                                           year=current_year,
#                                           month=current_month)

    # Buckt and file name with YYYYmm and cia name vars replaced
    for cia, id in zip(cia, key_id):
        #gdrive path
        gdrive = config_vars.clean_name_gdrive(var=gcs_file, 
                                               key="gdrive", 
                                               cia=cia, 
                                               year=current_year, 
                                               month=current_month, 
                                               path="gdrive")

        gdrive_rich = config_vars.clean_name_gdrive(var=gcs_file, 
                                               key="gdrive", 
                                               cia=cia, 
                                               year=current_year, 
                                               month=current_month, 
                                               path="gdrive_rich")


        # creating dataframe to add file into buckets  
        to_data_frame = get_gdrive_data.start_gdrive_extraction(
            gdrive_file_id=id, 
            credentials_drive="CREDENCIALS_DRIVE", 
            api_key="API_KEY")

        # Obtém os valores dos parâmetros
        df_gdrive = pd.DataFrame(to_data_frame)

        if extract_type == "gdrive":
            task_type = "GDrive"
            persister = task.start(ingestor_type="persister", task_type=task_type)
            if zone == "gcs_bucket_landzone":
                persister.operation_starter(bucket_name=bucket_name,
                                            df=df_gdrive,
                                            gcs_file_name=f"{gcs_zone}/{gdrive}")

            if zone == "gcs_bucket_richzone":
                persister.operation_starter(bucket_name=bucket_name,
                                            df=df_gdrive,
                                            gcs_file_name=f"{gcs_zone}/"
                                            f"{gdrive_rich}")
