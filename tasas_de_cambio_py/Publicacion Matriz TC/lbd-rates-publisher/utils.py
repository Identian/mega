import logging
import os
import boto3  
import base64
import json 
import sys

import paramiko


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_enviroment_variable(variable_key):
    """
    Obtiene la variable de entorno por medio de 'variable_key' 
    :param variable_key: (string) Contiene el nombre de la variable de entorno
    :return: (string) El valor de las variables de entorno
    """
    try:
        logger.info('[utils] (get_enviroment_variable) Obtener secreto ' + variable_key)
        variable_value = os.environ[str(variable_key)]
        return variable_value
    except Exception as e:
        logger.error('[utils] (get_enviroment_variable) No se encontró las variables de entorno' + str(e))
        

def get_secret(secret_region, secret_name):
    """
    Obtiene las credenciales que vienen del Secret Manager
    :param secret_region: (string) Nombre de la region donde se encuentran las credenciales
    :param secrete_name: (string) Nombre del secreto
    :return: (dict) Diccionario con las credenciales
    """
    session = boto3.session.Session()
    client_secrets_manager = session.client(service_name='secretsmanager', region_name=secret_region)
    secret_data = client_secrets_manager.get_secret_value(SecretId=secret_name)
    if 'SecretString' in secret_data:
        secret_str = secret_data['SecretString']
    else:
        secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[utils] (get_secret) Se obtuvo el secreto')
    return json.loads(secret_str)


def connection_sftp():
    try:
        secret_region = get_enviroment_variable('SECRET_DB_REGION')
        secret_sftp_isines = get_enviroment_variable('SECRET_SFTP')
        secret_sftp = get_secret(secret_region, secret_sftp_isines)
        logger.info(f"[utils.py](connection_sftp) Conectandose al SFTP...")
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=secret_sftp["hostname"],
                    port=secret_sftp["port"],
                    username=secret_sftp["username"],
                    password=secret_sftp["password"])
        # Abriendo conexion.
        sftp = ssh.open_sftp()
        logger.info("Se conecto correctamente al SFTP")
        route_path = secret_sftp["route_publish"]
        return sftp, route_path
    except Exception as e:
        line_exception = sys.exc_info()[2].tb_lineno
        logger.info(f"Hubo un error en la conexión SFTP: {e}, Línea {line_exception}")