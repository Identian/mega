"""
Módulo que se encarga de la generación de archivos de Hazzard Internacional
- Trae la información de base de datos, correspondiente a cada Hazzard
- Tablas: pub_otc_hazzard_rates, pub_otc_probabilities
- Envía los archivos correspondientes a la unidad DataNFS
"""

# Nativas
from base64 import b64decode
from dateutil import relativedelta as rd
import datetime as dt
from decimal import Decimal
import json
import logging
from sys import argv, exc_info, stdout

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client

# De terceros
import pandas as pd
import paramiko
import numpy as np
import sqlalchemy as sa


ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")

ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)
PARAMETER_STORE = "ps-otc-lambda-reports"

#-------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
_bootstrap_error_message = 'Fallo el calculo del error bootstrap'
_bootstrap_error_raise_message ="No se pudo calcular el error bootstrap para la curva con los insumos dados"
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
        raise Exception(error_msg) from sec_exc
        
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
        raise Exception(error_msg) from sec_exc
    

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
        raise

#---------------------------------------------------------------------------------
class SFTPHandler:
    """
    Representa el lector del archivo del SFTP
    Envía los archivos creados al servidor
    """
    
    def __init__(self, data_connection):
        self.data_connection = data_connection
        

    def connect_to_sftp(self, timeout: int = 40) -> paramiko.SSHClient:
        """
        Se conecta a un SFTP usando las credenciales contenidas en 'secret'
        y la libreria 'paramiko'. Las credenciales 'secret' se almacenan en Secrets Manager
    
        Returns:
            paramiko.SSHClient: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
        """
        raise_msg = "No fue posible conectarse al SFTP destino"
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
            self.sftp = self.ssh_client.open_sftp()
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise Exception(raise_msg) from unknown_exc

    def disconnect_sftp(self):
        """Cierra la conexión y la sesión hacia el SFTP"""
        if self.sftp is not None:
            self.sftp.close()
            self.ssh_client.close()
            logger.info("Se ha cerrado la conexion al SFTP")
        else:
            logger.info("No hay conexion al servidor SFTP")
    
    
    def load_files_to_sftp(self, df_file: pd.DataFrame, file_name: str, columns_file: list, route_sftp: str):
        """Genera y carga los archivos en el SFTP de Hazzard Internacional"""
        logger.info("Comenzando a generar el archivo %s en el sftp", file_name)
        error_message = "No se pudo generar el archivo en el SFTP"
        try:
            with self.sftp.open(route_sftp + file_name, "w") as f:
                try:
                    f.write(df_file.to_csv(index=False, sep=",", line_terminator='\r\n', header=True, columns=columns_file))
                except Exception as e:
                    logger.error(create_log_msg(f"Fallo la escritura del archivo: {file_name}"))
                    raise PlataformError("No fue posible la escritura del archivo: "+ str(e))
        except Exception as e:
            logger.error(create_log_msg(error_message+": "+file_name))
            raise PlataformError(f"No fue posible generar el archivo {file_name} en el SFTP: "+ str(e))
        
#-------------------------------------------------------------------------------------------------------------------
class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.connection = None
        self.valuation_date = valuation_date
        self.url_db = url_db
        
    
    def connect_db(self):
        """Genera la conexión a base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            logger.info("Se conecto correctamente a la base de datos")
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise PlataformError("Hubo un error en la conexión a base de datos: " + str(e))


    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            
            
    def get_data_hazzard(self, id_precia: str):
        """Trae el valor de los Hazzard Inter"""
        query_exchange_rate = sa.sql.text("""
            SELECT days, rate as hazard FROM pub_otc_hazzard_rates
            WHERE id_precia = :id_precia
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "id_precia": id_precia,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información de los Hazzard"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información del Hazzard: {id_precia}")
            if df_exchange_rate.empty:
                raise ValueError(f"No hay datos del CDS: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+id_precia))
            raise PlataformError(f"No fue posible extraer la información del Hazzard {id_precia}: "+ str(e))
        
    
    def get_data_probabilities(self, id_precia: str):
        """Trae el valor de las probabilities de default y supervivencia"""
        query_exchange_rate = sa.sql.text("""
            SELECT days as Dias, pd_value as Pr_default, 
            ps_value as Pr_Survival
            FROM pub_otc_probabilities
            WHERE id_precia = :id_precia
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "id_precia": id_precia,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información de las probabilidades"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información de las probabilidades: {id_precia}")
            if df_exchange_rate.empty:
                raise ValueError(f"No hay datos del CDS: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+id_precia))
            raise PlataformError(f"No fue posible extraer la información del Hazzard {id_precia}: "+ str(e))

#-------------------------------------------------------------------------------------------------------------------

def decimal_convert(num):
    if pd.notnull(num):
        dec_num = Decimal(str(num))
        return format(dec_num, 'f')
    else:
        return num

#-------------------------------------------------------------------------------------------------------------------
        
class ETL():
    """Representa la orquestación de la ETL"""
    def __init__(self, url_publish_otc: str) -> None:
        self.url_publish_otc = url_publish_otc
        valuation_date_param = get_params(["VALUATION_DATE"])
        self.valuation_date = valuation_date_param["VALUATION_DATE"]
        curve_param = get_params(["HAZZARD"])
        self.curve = curve_param["HAZZARD"]
        self.df_hazzard_co = pd.DataFrame()

    
    def extract_data(self):
        """Orquesta la extracción de la informacion de la clase DbHandler"""
        try:
            self.db_handler_pub = DbHandler(self.url_publish_otc, self.valuation_date)
            self.db_handler_pub.connect_db()
            self.df_hazzard = self.db_handler_pub.get_data_hazzard(self.curve)
            self.df_probabilities = self.db_handler_pub.get_data_probabilities(self.curve)
            self.db_handler_pub.disconnect_db()
            logger.info(self.df_hazzard.to_string())
            logger.info(self.df_probabilities.to_string())
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            raise PlataformError(e.error_message)
            
        
    def transform_data(self):
        """Orquesta la construcción de los Hazzard, Probabilidades de Default y Supervivencia"""
        try:
            logger.info("transform")
            if self.curve == 'CO':
                self.df_hazzard_co = self.df_hazzard[self.df_hazzard['days'] < 1825]
                
            self.df_hazzard['hazard'] = self.df_hazzard['hazard'].apply(decimal_convert)
            self.df_probabilities['Pr_default'] = self.df_probabilities['Pr_default'].apply(decimal_convert)
            self.df_probabilities['Pr_Survival'] = self.df_probabilities['Pr_Survival'].apply(decimal_convert)
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            raise PlataformError(e.error_message)


    def load_info(self, data_connection_sftp, route_hazzard, route_pds):
        """Orquesta la generación de archivos de Hazzard Inter y Probabilidades de Default"""
        try:
            loader = SFTPHandler(data_connection_sftp)
            formatted_date = self.valuation_date.replace("-", "")
            file_name_haz = 'Haz_{}_Nodos_{}.csv'.format(self.curve, formatted_date)
            file_name_pd = 'Pd_{}_Nodos_{}.csv'.format(self.curve, formatted_date)
            file_name_sp = 'Sp_{}_Nodos_{}.csv'.format(self.curve, formatted_date)
            file_name_fp = 'Haz_FP_Nodos_{}.csv'.format(formatted_date)
            loader.connect_to_sftp()
            loader.load_files_to_sftp(self.df_hazzard, file_name_haz, ['days', 'hazard'], route_hazzard)
            loader.load_files_to_sftp(self.df_probabilities, file_name_pd, ['Dias', 'Pr_default'], route_pds)
            loader.load_files_to_sftp(self.df_probabilities, file_name_sp, ['Dias', 'Pr_Survival'], route_pds)
            if self.curve == 'CO':
                loader.load_files_to_sftp(self.df_hazzard_co, file_name_fp, ['days', 'hazard'], route_hazzard)
            loader.disconnect_sftp()
            
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            raise PlataformError(e.error_message)
        finally:
            loader.disconnect_sftp()
    
def main():
    params_key = [
        "HAZZARD",
        "VALUATION_DATE",
        "DATANFS_SECRET",
        "DB_SECRET"
    ]
    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)
    url_publish_otc = key_secret_db["conn_string_aurora_publish"]
    schema_publish_otc = key_secret_db["schema_aurora_publish"]
    url_db_publish_aurora = url_publish_otc+schema_publish_otc
    
    secret_sftp = params_glue["DATANFS_SECRET"]
    key_secret_sftp = get_secret(secret_sftp)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_hazzard = key_secret_sftp["route_hazzard"]
    route_pds = key_secret_sftp["route_pds"]
    
    
    
    etl =  ETL(url_db_publish_aurora)
    etl.extract_data()
    etl.transform_data()
    etl.load_info(data_connection_sftp, route_hazzard, route_pds)
    
    
#-------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    
    main()
