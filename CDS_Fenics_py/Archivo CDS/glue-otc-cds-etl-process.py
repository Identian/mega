"""
Módulo para la recolección los datos CDS
- Recolecta la entrada instument de la base de datos 
- Genera los archivos correspondientes para cada instument
- Envía los archivos correspondientes la servidor FTP
"""

from awsglue.utils import getResolvedOptions
from base64 import b64decode
from boto3 import client as aws_client
from datetime import datetime
from json import loads as json_loads, dumps as json_dumps
from sys import argv, exc_info, stdout

import ftplib
import logging
import pandas as pd
import paramiko
import smtplib
import sqlalchemy as sa

ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")

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

#-------------------------------------------------------------------------------------------------------------------
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
    except Exception as sec_exc:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}")
        logger.error(create_log_msg(error_msg))
        raise Exception(error_msg) from sec_exc

#-------------------------------------------------------------------------------------------------------------------        
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
        return json_loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise Exception(error_msg) from sec_exc

#-------------------------------------------------------------------------------------------------------------------
class SFTPFileConnectorUtils:
    """
    Representa el lector del archivo del SFTP
    Envía los archivos creados al servidor
    """
    
    def __init__(self, data_connection):
        self.data_connection = data_connection
        
    def connect_to_sftp(self, timeout: int = 2) -> paramiko.SSHClient:
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
        
#-------------------------------------------------------------------------------------------------------------------
class DBConnectorUtils:
    """
    Representa el lector de la base de datos
    Lee los datos de la tabla
    """

    CONN_CLOSE_ATTEMPT_MSG = "Intentando cerrar la conexion a BD"
    def __init__(self, db_url_src : str) -> None:
        """Gestiona la conexion y las transacciones a base de datos

        Args:
            db_url_src  (str): url necesario para crear la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url_src  = db_url_src 
        except (Exception,) as init_exc:
            error_msg = "Fallo la creacion del objeto DBManager"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from init_exc
        
    def create_connection(self) -> sa.engine.Connection:
        """Crea la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion de la conexion a BD

        Returns:
            sa.engine.Connection: Conexion a BD
        """
        try:
            logger.info("Creando el engine de conexion...")
            sql_engine = sa.create_engine(
                self.db_url_src , connect_args={"connect_timeout": 2}
            )
            logger.info("Engine de conexion creado con exito")
            logger.info("Creando conexion a BD...")
            db_connection = sql_engine.connect()
            logger.info("Conexion a BD creada con exito")
            return db_connection
        except (Exception,) as conn_exc:
            raise_msg = "Fallo la creacion de la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc
        
    def get_data(self, instrument, valuation_date):
        select_query = ("SELECT days, mid_price "+ 
                        "FROM prc_otc_cds "+
                        "WHERE counterparty = '{}' ".format(instrument)+
                        "AND valuation_date = '{}' ".format(valuation_date)+
                        "AND status_info = 1; ")
        db_connection = self.create_connection()
        fwd_params_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return fwd_params_df 
    
#-------------------------------------------------------------------------------------------------------------------
#PRECIA_UTILS_EXCEPTIONS 
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""
    
#-------------------------------------------------------------------------------------------------------------------
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
def main():
    parameters = [
        "INSTRUMENT",
        "VALUATION_DATE",
        "DATANFS_SECRET",
        "DB_SECRET"
    ]
    params_glue = get_params(parameters)
    instrument = params_glue["INSTRUMENT"]
    valuation_date = params_glue["VALUATION_DATE"]      

    sftp_secret = params_glue["DATANFS_SECRET"]
    key_secret_sftp = get_secret(sftp_secret)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_cds = key_secret_sftp["route_cds"]
    
    db_secret = params_glue["DB_SECRET"]
    db_url_dict = get_secret(db_secret)
    db_url_sources = db_url_dict["conn_string_aurora_process"]
    schema_utils = db_url_dict["schema_process"]
    db_url_utils = db_url_sources + schema_utils
    body_error = "La recolección del archivo de CDS del SFTP Fenics ha fallado: "
     
    try:
        sftp_reader = SFTPFileConnectorUtils(data_connection_sftp)
        sftp_reader.connect_to_sftp()
        
        instrument_list = instrument.split(',')
        logger.info("Conexion al SFTP exitosa.")
        for instrument_iterator in instrument_list:    
            actiondb = DBConnectorUtils(db_url_utils)
            resultquery = actiondb.get_data(instrument_iterator, valuation_date)
    
            formatted_date = valuation_date.replace("-", "")
            file_name = 'Cds_{}_Nodos_{}.txt'.format(instrument_iterator, formatted_date)
            
            with sftp_reader.sftp.open(route_cds + file_name, 'w') as file_result:
                file_result.write(resultquery.to_csv(index=False, line_terminator='\r\n', header=False, sep=' '))
                    
            #sftp_reader.sftp.put(file_name, route_cds + file_name)
        
        sftp_reader.disconnect_sftp()
    except Exception as e:
        error_message = "No se pudo obtener el archivo del SFTP de Fenics"
        body_error = body_error + str(e)
        raise Exception(error_message) from e
    finally:
        sftp_reader.disconnect_sftp()
    
    
#-------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    
    main()
