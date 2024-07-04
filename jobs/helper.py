
import json
import os


class Helper:
    # Loading variables .env
    def get_env_variables(self, var: str) -> str:
        from dotenv import load_dotenv
        """
        Read a .env file and return its value as a string.
        Args:
            var (str): The name of the variable from .env.  
        Returns:
            str: The variable's value.
        """
        load_dotenv()
        get_env = os.getenv(var)
        
        return get_env

    # Read a JSON
    @staticmethod
    def get_customer_info(file_name: str, key: str=None) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_vars_jobs = os.path.abspath(file_name)
        with open(conf_vars_jobs, "r") as file:
            data = json.load(file)
            customer_info = str(list(data.keys())[0])

        return data[key]

    @staticmethod
    def get_email_body(file_name: str):
        with open(file_name, 'r') as file:
            # Ler o conteúdo do arquivo inteiro
            content = file.read()
        
        return content

    @staticmethod
    def get_query_string(file_name: str) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_vars_jobs = os.path.abspath(file_name)
        # Abrindo e lendo o arquivo .sql
        with open(conf_vars_jobs, 'r', encoding='utf-8') as arquivo:
            conteudo_sql = arquivo.read()

        # Exibindo o conteúdo
        return conteudo_sql

    # Read dalk json conf
    @staticmethod
    def get_dlk_vars(file_name: str, env: str, zone: str) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
            env: (str) Envirioment that is being runnig
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_dlk_vars = os.path.abspath(f"config/{env}/{file_name}")
        if zone == "project_config":
            with open(conf_dlk_vars, "r") as file:
                data = json.load(file)
                zone = str(list(data.keys())[0])
            
            return data[zone]
        
        elif zone == "gcs_bucket_landzone" or zone == "gcs_bucket_richzone":
            if "gcs_bucket_landzone" == zone:
                index = 1
            if "gcs_bucket_richzone" == zone:
                index = 2

            with open(conf_dlk_vars, "r") as file:
                data = json.load(file)
                zone = str(list(data.keys())[index])

        return data[zone]

    @staticmethod
    def clean_name(var: str, 
                   key: str, 
                   index: int, 
                   cia: str, 
                   year: str = None, 
                   month: str = None):
        """
        Replace variables and clean dlk file path.
        Args:
            var (str): Var that goes before the key.  
            key (str): key to get datalake parameter
            cia (str): Company name
            year (str): Curretly year
            month (str): Currenty month
        Returns:
            dict: The path cleaned
        """
        path = var[key][index].replace(
            "%cia", str(cia)).replace(
                "%YYYY", str(year)).replace(
                    "%mm",str(month).zfill(2))
        return path

    @staticmethod
    def clean_name_gdrive(var: str, 
                   key: str, 
                   cia: str, 
                   year: str, 
                   month: str,
                   path: str = None):

        if path == "gdrive":
            path_lz = var[key].replace(
                "%cia", str(cia).lower())
            return path_lz

        elif path == "gdrive_rich":
            path_rich = var[key].replace(
                "%cia",  str(cia).lower()).replace(
                    "%YYYY",  str(year)).replace(
                        "%mm",  str(month).zfill(2))
            return path_rich

    @staticmethod
    def rich_params(file_name: str) -> str:
        """
        Read a JSON file and return its data as a dictionary.
        Args:
            file_name (str): The name of the JSON file.  
        Returns:
            dict: The data from the JSON file as a dictionary.
        """
        conf_vars_jobs = os.path.abspath(file_name)
        with open(conf_vars_jobs, "r") as file:
            data = json.load(file)
            customer_info = str(list(data.keys())[0])

        return data#[customer_info]
