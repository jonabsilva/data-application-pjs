from dotenv import load_dotenv
import json
import os


# Loading variables .env
def get_env_variables(var: str) -> str:
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
def get_customer_info(file_name: str) -> str:
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
        field_id = str(list(data.keys())[0])
    
    return data[field_id]

#print(list(set(get_customer_info("config/conf-vars.json").values())))