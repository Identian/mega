"""
Glue que se encarga de la publicación de los archivos swap internacionales
"""
# Nativas
from base64 import b64decode
from datetime import datetime
import hashlib
import json
import logging
import re
from sys import argv, exc_info, stdout

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client

# De terceros
import paramiko
import pandas as pd
import numpy as np
import sqlalchemy as sa

ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)
PARAMETER_STORE = "ps-otc-lambda-reports"

#-------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
def setup_logging(log_level):
    """
    Configura el sistema de registro de logs.
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        file_handler = logging.StreamHandler(stdout)
    file_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato adecuado al mensaje log_msg, incluyendo información sobre excepciones.
    """
    exception_type, exception_value, exception_traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )
    
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


class UserError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados
    por la informacion suministrada en el event
    """

    def __init__(
        self,
        error_message="El event no tienen la estructura y/o valores esperados",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


class PlataformError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados por
    errores del sistema identificados
    """

    def __init__(
        self,
        error_message="La plataforma presenta un error, ver el log para mas detalles",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)
        

#-------------------------------------------------------------------------------------------------------------------
# OBTENER SECRETOS Y PARÁMETROS
def get_params(parameter_list) -> dict:
    """Obtiene los parametros de entrada del glue

    Parameters:
        parameter_list (list): Lista de parametros
    
    Returns:
        dict: Valor de los parametros
    """
    try:
        logger.info("Obteniendo parametros del glue job ...")
        params = getResolvedOptions(argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except Exception as e:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}")
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from e
        
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Parameters:
        secret_name (str): Nombre del secreto en el servicio AWS.
        
    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = aws_client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc
    

def get_parameter_store(parameter_name):
    """
    Obtiene el valor del parameter store
    
    Parameters:
        parameter_name (str): Nombre del parámetro
    
    Returns:
        str: valor del parametro obtenido
    """
    logger.info("Intentando leer el parametro: "+parameter_name)
    ssm_client = aws_client('ssm')
    response = ssm_client.get_parameter(
        Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " +
                str(response['Parameter']['Value']))
    return response['Parameter']['Value']

#--------------------------------------------------------------------------------
# EJECUTAR LAMBDA
def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Parameters:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = aws_client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from lbd_exc
    
#---------------------------------------------------------------------------------
# REPORTE DE ESTADOS
def update_report_process(input_id, output_id, valuation_date, status, description, technical_description):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """

    lambda_name = get_parameter_store(PARAMETER_STORE)
    report_process = {
          "input_id": input_id,
          "output_id":output_id,
          "process": "Derivados OTC",
          "product": "swp_inter",
          "stage": "Publicacion",
          "status": "",
          "aws_resource": "glue-p-otc-swp-inter-publish",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")
#------------------------------------------------------------------------------------------------------------
# DESCARGAR ARCHIVOS A S3
def download_file_s3(bucket_name, key):
    s3 = aws_client("s3")
    try:
        logger.info('Descargando el archivo en el S3...')
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)
        file_origin = s3_object['Body'].read()
        logger.info('Se descarga el archivo del S3')
        return file_origin
    except Exception as e:
        error_msg = "No se logro  descargar los archivos en el S3"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from e

#------------------------------------------------------------------------------------------------------------
# CARGAR ARCHIVOS AL S3
def upload_s3(bucket_name, data, file_name):
    """Se sube los archivos al bucket de validacion"""
    path_s3_validator = "Swap_Inter"
    logger.info("Cargando el archivo en el S3...")
    try:
        s3 = aws_client('s3')
        s3.put_object(Body=data, Bucket=bucket_name, Key=f"{path_s3_validator}/{file_name}")
        logger.info(f"Se ha cargado el archivo {file_name} en el bucket {bucket_name} correctamente")
    except Exception as e:
        error_msg = f"Ocurrio un error en la carga del archivo del bucket {bucket_name}. ERROR: {str(e)}"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from e

#------------------------------------------------------------------------------------------------------------
# GENERACIÓN SHA
def generate_sha256_from_s3_file(data_file, file_name):
    """Genera el sha256 del archivo original que viene de s3"""
    try:
        hash_file_name = "FC_"+file_name+'.sha256'
        hash_value = hashlib.sha256(data_file).hexdigest()
        logger.info(f"Se ha generado el sha correctamente del archivo {file_name}")
        return hash_value, hash_file_name
    except (Exception, ) as e:
        error_msg = f"Ocurrio un error en la generacion del sha del archivo {file_name}"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from e
        
#------------------------------------------------------------------------------------------------------------
# CARGA DE INFORMACIÓN
class Loader:
    """Representa la publicaión de los archi de Swap Inter"""

    def __init__(self, data_connection_sftp: dict, route_publish: str) -> None:
        self.data_connection = data_connection_sftp
        self.route_path = route_publish


    def connect_sftp(self, timeout: int = 40) -> paramiko.SSHClient:
        """
        Se conecta a un SFTP usando las credenciales contenidas en 'secret'
        y la libreria 'paramiko'. Las credenciales 'secret' se almacenan en Secrets Manager
    
        Returns:
            paramiko.SSHClient: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
        """
        try:
            logger.info("Conectandose al SFTP ...")
            logger.info("Validando secreto del SFTP ...")
            sftp_host = self.data_connection["host"]
            sftp_port = self.data_connection["port"]
            sftp_username = self.data_connection["username"]
            sftp_password = self.data_connection["password"]
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
            self.ssh_client.connect(
                sftp_host,
                port=sftp_port,
                username=sftp_username,
                password=sftp_password,
                timeout=timeout,
            )
            logger.info("Conexion al SFTP exitosa.")
            self.sftp_connect = self.ssh_client.open_sftp()
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from unknown_exc
    
    
    def disconnect_sftp(self):
        """Cierra la conexión al SFTP"""
        if self.sftp_connect is not None:
            self.sftp_connect.close()
            self.sftp_connect = None


    def move_to_sftp(self, data, name_file, valuation_date):
        """¨Publica los archivos y el sha en el FTP de clientes"""
        route_path = self.route_path+valuation_date+"/"+name_file
        logger.info(f"Fecha carpeta a publicar {valuation_date}")
        try:
            with self.sftp_connect.open(route_path, 'wb') as f:
                f.write(data)
            logger.info(f"Se ha publicado el archivo {name_file} correctamente")
        except Exception as e:
            logger.error(f"Ocurrio un error en la publicacion del archivo: {name_file}, {str(e)}")
    
    
    def download_ftp(self, name_file, valuation_date):
        """Descarga el archivo y sha de FTP de clientes"""
        route_path = self.route_path+valuation_date+"/"+name_file
        try:
            with self.sftp_connect.open(route_path, 'rb') as remote_file:
                file_data = remote_file.read()
            logger.info("Se ha descargado el archivo {name_file} correctamente del FTP")
            return file_data
        except Exception as e:
            logger.error(f"Ocurrio un error en la descarga del FTP del archivo: {name_file}: {str(e)}")
            
#-----------------------------------------------------------------------------------------
class ETL():
    """Representa la orquestación de la ETL"""
    def __init__(self, files_publish_list, files_publish) -> None:
        s3_bucket_name_param = get_params(["S3_BUCKET"])
        self.s3_bucket_name = s3_bucket_name_param["S3_BUCKET"]
        s3_bucket_validator_param = get_params(["S3_BUCKET_VALIDATOR"])
        self.s3_bucket_validator = s3_bucket_validator_param["S3_BUCKET_VALIDATOR"]
        self.files_publish_list = files_publish_list
        self.files_publish = files_publish


    def load_info(self, data_connection_sftp, files_publish, route_publish):
        """Orquesta la publicación de los archivos"""
        date_pattern = re.compile(r"\d{8}")
        logger.info("Comienza la publicación de los archivos Swap Inter... ")
        loader = Loader(data_connection_sftp, route_publish)
        loader.connect_sftp()
        logger.info(f"Lista de archivos: {files_publish}")
        for file in files_publish:
            logger.info(f"Archivo a publicar: {file}")
            coincidencias = date_pattern.search(file)
            if coincidencias:
                date_extracted = coincidencias[0]
                valuation_date = datetime.strptime(date_extracted, "%Y%m%d").strftime("%Y-%m-%d")
            s3_path = "Publish/"+file
            
            try:
                data_file = download_file_s3(self.s3_bucket_name, s3_path)
                hash_value, hash_file_name = generate_sha256_from_s3_file(data_file, file)
                loader.move_to_sftp(data_file, file, valuation_date)
                loader.move_to_sftp(hash_value, hash_file_name, valuation_date)
                data_original = loader.download_ftp(file, valuation_date)
                data_hash = loader.download_ftp(hash_file_name, valuation_date)
                logger.info("Comienza la validación de las firmas...")
                upload_s3(self.s3_bucket_validator, data_original, file)
                upload_s3(self.s3_bucket_validator, data_hash, hash_file_name)
            except PlataformError as e:
                logger.error(create_log_msg(e.error_message))
                update_report_process(self.files_publish_list, self.files_publish, valuation_date, "Fallido", "Fallo la publicación", str(e))
                raise PlataformError(e.error_message)
        loader.disconnect_sftp()
        update_report_process(self.files_publish_list, self.files_publish, valuation_date, "Exitoso", "Proceso Finalizado", "")

def main():
    ftp_secret_param = get_params(["FTP_SECRET"])

    secret_sftp = ftp_secret_param["FTP_SECRET"]
    key_secret_sftp = get_secret(secret_sftp)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_publish = key_secret_sftp["route_publish"]

    list_files_param = get_params(["FILE_LIST"])
    files_publish_list = list_files_param["FILE_LIST"]
    logger.info(type(files_publish_list))
    files_publish = files_publish_list.split(',')
    logger.info(files_publish)
    
    etl = ETL(files_publish_list, files_publish)
    etl.load_info(data_connection_sftp, files_publish, route_publish)



if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main() 