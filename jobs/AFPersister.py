from __future__ import annotations
from abc import ABC, abstractmethod

from datetime import datetime
import pandas as pd

from scripts.processing.datalake.ingestor import *
from jobs.AFEsaj import *
from jobs.AFGdrive import *
from jobs.helper import Helper
from jobs.AFExtraction import (IIngestion, Extraction)

import ssl
# Credentials - Certificate
ssl._create_default_https_context = ssl._create_unverified_context

# Get current date
current_date = datetime.now()
# Get current year and month
current_year = current_date.year
current_month = current_date.month

code_process_list = ["1028260-20.2021.8.26.0007", "1008436-85.2024.8.26.0002"]


## O CLIENT SERA CHAMDO DO CONCRETE PRODUCT
def af_persistent_client_code(gcs_bucket: str,
                               extract_type: str = None) -> None:
     """
     The client code works with factories and products only through abstract
     types: IIngestion and AbstractProduct. This lets you pass any factory
     or product subclass to the client code without breaking it.
     """
     # from simple factory
     task = IngestorOperator()
     config_vars = Helper()

     conf = config_vars.rich_params("config/dev/dlk-vars.json")
     dlk_vars = config_vars.rich_params("config/conf-vars.json")
     cia = [cia for cia in dlk_vars.keys()]


     # CABECALHO
     cd_cabecalho = ESajData()
     for process in code_process_list:
         df_cd_cabecalho = cd_cabecalho.get_cabecalho(process)
         df_cabecalho = cd_cabecalho.start_scraping(dataframe=df_cd_cabecalho)

         columns = df_cabecalho.columns
         values = df_cabecalho.values[0]

         # Creating de um dict from the list
         data = dict(zip(columns, values))
         df_table_cab = pd.DataFrame([data]).drop('Distribuicao', axis=1)
         df_table_cab['Valor_da_acao'] = df_table_cab['Valor_da_acao'].astype(str)
         df_table_cab['Valor_da_acao'] = df_table_cab['Valor_da_acao'].str[28:-6].str.strip()

         cabecalho = config_vars.clean_name(var=conf['gcs_bucket_richzone'],
                                         key="esaj",
                                         index=0,
                                         cia=cia,
                                         year=current_year,
                                         month=current_month)
         query_string = "scripts/processing/datalake/sql/cabecalho.sql"
         loader = task.start(ingestor_type="persister" ,task_type=extract_type)

         for cias in dlk_vars['gdrive_file_id'].keys():
             name = f"{str(cias).lower()}/{cabecalho}"
             process_code=process.replace('.','')
             gcs_file_name=f"{name[:-4]}_{process_code}.parquet"
             
             loader.operation_starter(
             query_file= config_vars.get_query_string(query_string),
             bucket_name=gcs_bucket, 
             df=df_table_cab, 
             gcs_file_name=gcs_file_name)
             # Buckt and file name with YYYYmm and cia name vars replaced
     
     # CODE TO BE USED WITH THE FILE NAME
     process_code_file_name = process_code

     # MOVIMENTACOES
     cd_movimentacoes = ESajData()
     for process in code_process_list:
         df_cd_movimentacoes = cd_movimentacoes.get_movimentacoes(process)
         df_movimentacoes = cd_movimentacoes.start_scraping(dataframe=df_cd_movimentacoes)

         # Creating de um dict from the list
         data = dict(zip(columns, values))
         df_table_mvnt = pd.DataFrame(df_movimentacoes)
         df_table_mvnt["Codigo_do_Processo"] = process

         movimentacoes = config_vars.clean_name(var=conf['gcs_bucket_richzone'],
                                         key="esaj",
                                         index=1,
                                         cia=cia,
                                         year=current_year,
                                         month=current_month)
         query_string = "scripts/processing/datalake/sql/movimentacoes.sql"
         loader = task.start(ingestor_type="persister" ,task_type=extract_type)

         for cias in dlk_vars['gdrive_file_id'].keys():
             name = f"{str(cias).lower()}/{movimentacoes}"
             process_code=process.replace('.','')
             gcs_file_name=f"{name[:-4]}_{process_code}.parquet"
             
             loader.operation_starter(
             query_file= config_vars.get_query_string(query_string),
             bucket_name=gcs_bucket, 
             df=df_table_mvnt, 
             gcs_file_name=gcs_file_name)

     # PARTES DO PROCESSO
     cd_process_part = ESajData()
     for process in code_process_list:
         df_cd_process_part = cd_process_part.get_process_part(process)
         df_process_part = cd_process_part.start_scraping(dataframe=df_cd_process_part)

         columns = df_process_part.columns
         values = df_process_part.values[0]

         # Creating de um dict from the list
         data = dict(zip(columns, values))
         df_table_cdp = pd.DataFrame([data])
         df_table_cdp["Codigo_do_Processo"] = process

         process_part = config_vars.clean_name(var=conf['gcs_bucket_richzone'],
                                         key="esaj",
                                         index=2,
                                         cia=cia,
                                         year=current_year,
                                         month=current_month)
         query_string = "scripts/processing/datalake/sql/process_part.sql"
         loader = task.start(ingestor_type="persister" ,task_type=extract_type)

         for cias in dlk_vars['gdrive_file_id'].keys():
             name = f"{str(cias).lower()}/{process_part}"
             process_code=process.replace('.','')
             gcs_file_name=f"{name[:-4]}_{process_code}.parquet"
             
             loader.operation_starter(
             query_file= config_vars.get_query_string(query_string),
             bucket_name=gcs_bucket, 
             df=df_table_cdp, 
             gcs_file_name=gcs_file_name)

     # PETICOES DIVERSAS
     cd_peticoes_diversas = ESajData()
     for process in code_process_list:
         df_cd_peticoes_diversas = cd_peticoes_diversas.get_movimentacoes(process)
         df_peticoes_diversas = cd_peticoes_diversas.start_scraping(dataframe=df_cd_peticoes_diversas)

         df_table_dstr = pd.DataFrame(df_peticoes_diversas)
         df_table_dstr["Codigo_do_Processo"] = process

         peticoes_diversas = config_vars.clean_name(var=conf['gcs_bucket_richzone'],
                                         key="esaj",
                                         index=3,
                                         cia=cia,
                                         year=current_year,
                                         month=current_month)
         query_string = "scripts/processing/datalake/sql/peticoes_diversas.sql"
         loader = task.start(ingestor_type="persister" ,task_type=extract_type)

         for cias in dlk_vars['gdrive_file_id'].keys():
             name = f"{str(cias).lower()}/{peticoes_diversas}"
             process_code=process.replace('.','')
             gcs_file_name=f"{name[:-4]}_{process_code}.parquet"
             
             loader.operation_starter(
             query_file= config_vars.get_query_string(query_string),
             bucket_name=gcs_bucket, 
             df=df_table_dstr, 
             gcs_file_name=gcs_file_name)
