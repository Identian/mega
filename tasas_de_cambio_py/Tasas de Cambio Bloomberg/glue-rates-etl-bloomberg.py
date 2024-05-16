"""
Módulo para la recolección las tasas de Bloomberg
- Recolecta los archivos de tasas de Bloomberg
- Inserta los datos en la base de datos
- Actualiza el estado del proceso en la tabla de control
"""

from awsglue.utils import getResolvedOptions
from base64 import b64decode
from boto3 import client as aws_client
from datetime import datetime
from json import loads as json_loads, dumps as json_dumps
from sys import argv, exc_info, stdout

import boto3
import datetime
import io
import json
import logging
import pandas as pd
import sqlalchemy as sa

ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")
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

#--------------------------------------------------------------------------------------------------------------------
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


def is_friday(date_str):    
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.weekday() == 4

def next_day(date_string, number_of_days):
    date = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    next_date = date + datetime.timedelta(days=number_of_days)
    return next_date.strftime('%Y-%m-%d')

def call_lambda(payload, lambda_name):
    # Create an AWS Lambda client
    client = boto3.client('lambda')

    # Invoke the Lambda function
    response = client.invoke(
        FunctionName = lambda_name,
        InvocationType = 'RequestResponse',
        Payload = json_dumps(payload)
    )

    # Print the response from the Lambda function
    logger.info(response['Payload'].read().decode())
    
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
        
    def insert_data(self, query):
        """
        Se realiza la insercion de la información consultada
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                db_connection.execute(query)
                logger.info(self.CONN_CLOSE_ATTEMPT_MSG)
                db_connection.close()
                logger.info("Conexion a BD cerrada con exito")
                logger.info("Insercion de dataframe en BD exitosa")
            except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
    
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
        "BUCKET_NAME",
        "FILE_NAME",
        "VALUATION_DATE",
        "DB_SECRET",
        "RATE_PARITY_LAMBDA"
    ]
    params_glue = get_params(parameters)
    bucket_name = params_glue["BUCKET_NAME"]
    file_name = params_glue["FILE_NAME"]
    valuation_date = params_glue["VALUATION_DATE"]   
    
    db_secret = params_glue["DB_SECRET"]
    db_url_dict = get_secret(db_secret)
    db_url_sources = db_url_dict["conn_string_aurora_process"]
    schema_utils = db_url_dict["schema_process"]
    db_url_utils = db_url_sources + schema_utils

    lambda_parity = params_glue["RATE_PARITY_LAMBDA"]
    body_error = "La recolección del archivo de CDS del SFTP Fenics ha fallado: "
    
    try:
        s3 = boto3.client('s3')
        lambda_report = get_parameter_store(PARAMETER_STORE)
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read().decode('utf-8')
        #file_content = io.StringIO(file_content)
        
        # read CSV file from string
        document_file = pd.read_csv(io.StringIO(file_content))
        
        # Convert DataFrame to list of tuples
        tuple_list = list(document_file.to_records(index=False))
        
        query_builder = "INSERT INTO src_exchange_rates (id_precia, id_supplier, value_rates, valuation_date, effective_date, status_info) VALUES "             
        
        formatted_date = valuation_date.replace("-", "")

        for element in tuple_list:
            query_builder += "('{}', '{}', '{}', '{}', '{}', '{}'),".format(element[0].replace(" Curncy", ""), "Bloomberg", element[2], formatted_date, formatted_date, 1)
            if(is_friday(valuation_date)):  
                query_builder += "('{}', '{}', '{}', '{}', '{}', '{}'),".format(element[0].replace(" Curncy", ""), "Bloomberg", element[2], next_day(valuation_date, 1), next_day(valuation_date, 1), 1)
                query_builder += "('{}', '{}', '{}', '{}', '{}', '{}'),".format(element[0].replace(" Curncy", ""), "Bloomberg", element[2], next_day(valuation_date, 2), next_day(valuation_date, 2), 1)
                
        query_insert_new_records = "".join(query_builder)[:-1] + ";"
        
        logger.info(query_insert_new_records)
        
        query_invalidate_previous_records = ("UPDATE src_exchange_rates " +
            "SET status_info= 0 " +
            "WHERE id_precia IN ('USDCNH', 'USDARS', 'CLFCLP', 'USDVEF', 'USDPYG', 'USDTWD', 'USDANG', 'USDXCD', 'USDQAR', 'USDRUB', 'USDDOP') " + 
            "AND status_info = 1 " +   
            "AND valuation_date = " + formatted_date + " " +
            "AND id_supplier = 'bloomberg';")
        
        logger.info(query_insert_new_records)
        
        actiondb = DBConnectorUtils(db_url_utils)        
        actiondb.insert_data(query_invalidate_previous_records)
        actiondb.insert_data(query_insert_new_records)
        
        logger.info("Inserción de datos satisfactoria.")        

        # Define the input payload for the Lambda function
        valuation_date_list = valuation_date
        if(is_friday(valuation_date)):
            valuation_date_list += " " + next_day(valuation_date, 1) + " " + next_day(valuation_date, 2)
        payload = {
            "valuation_date_list": valuation_date_list
        }
        
        call_lambda(payload, lambda_parity)
        
        payload = {
            "input_id": "Rates_Bloomberg",
            "output_id": [
                "MatrizTC"
            ],
            "process": "rates",
            "product": "rates",
            "stage": "Recoleccion",
            "status": "Exitoso",
            "aws_resource": "glue-p-rates-etl-bloomberg",
            "type": "insumo",
            "description": "Proceso Finalizado",
            "technical_description": "Proceso Finalizado",
            "valuation_date": valuation_date
        } 
        
        call_lambda(payload, lambda_report)
        
    except Exception as e:
        error_message = "No se pudo actualizar la información en la base de datos."
        body_error = body_error + str(e)
        payload = {
            "input_id": "Rates_supplier",
            "output_id": [
                "MatrizTC"
            ],
            "process": "rates",
            "product": "rates",
            "stage": "Recoleccion",
            "status": "Fallido",
            "aws_resource": "glue-p-rates-etl-bloomberg",
            "type": "insumo",
            "description": "Ha ocurrido un error al intentar insertar las tasas de Bloomberg en la base de datos.",
            "technical_description": body_error,
            "valuation_date": valuation_date
        } 
        call_lambda(payload, lambda_report)
        raise Exception(error_message) from e    
        
    
#-------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    
    main()
