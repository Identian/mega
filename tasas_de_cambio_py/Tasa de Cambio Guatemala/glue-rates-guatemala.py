"""
=============================================================

Nombre: glue-rates-guatemala.py
Tipo: Glue Job

Autores:
    - Lorena Julieth Torres Hernández
Tecnología - Precia

Ultima modificación: 12/11/2023

En el presente script se realiza la recolección de la tasa del
Banco central de Guatemala, insertando el dato en base de datos
y lanzando el porceso de paridad de tasas 
=============================================================
"""


#Nativas de Python
from datetime import datetime, timedelta, timezone, date
import logging
import json
import sys
from sys import exc_info, stdout
import base64

#AWS
import boto3
from awsglue.utils import getResolvedOptions 

#De terceros
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import requests
import xml.etree.ElementTree as ET


date_format = "%Y-%m-%d"
ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)


def setup_logging(log_level):
    """
    formatea todos los logs que invocan la libreria logging
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = logging.StreamHandler(stdout)
    precia_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato de la variable ERROR_MSG_LOG_FORMAT al mensaje log_msg.
    Valida antes de crear el mensaje si existe una excepcion, y de ser el caso
    asocia el mensaje log_msg a los atributos de la excepcion.

    Args:
        log_msg (str): Mensaje de error personalizado que se integrara al log

    Returns:
        str: Mensaje para el log, si hay una excepcion responde con el
        formato ERROR_MSG_LOG_FORMAT
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


#PRECIA_UTILS_AWS
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Args:
        secret_name (str): Nombre del secreto en el servicio AWS.

    Raises:
        PlataformError: Cuando ocurre algun error al obtener el secreto.

    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = boto3.client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = base64.b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


def get_params(parameter_list: list) -> dict:
    """
    Obtiene los parametros configurados en 'Job parameters' en el Glue Job

    Args:
        parameter_list (list): Lista de parametros a recuperar

    Returns:
        dict: diccionario de los parametros solicitados
    """
    try:
        logger.info("Obteniendo parametros ...")
        params = getResolvedOptions(sys.argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except (Exception,) as sec_exc:
        error_msg = (
            f"No se encontraron todos los parametros solicitados: {parameter_list}"
        )
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


logger = setup_logging(logging.INFO)



class get_data():
    def get_data_gtq(self):
        try:
            logger.info('Se inicia con la lectura del web service del banco dentral de Guatemala...')
            soap_request = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <TipoCambioDia xmlns="http://www.banguat.gob.gt/variables/ws/" />
            </soap:Body>
            </soap:Envelope>
            """
            url_gtq = "http://banguat.gob.gt/variables/ws/TipoCambio.asmx"
            headers = {
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": "http://www.banguat.gob.gt/variables/ws/TipoCambioDia",
            }
            response = requests.post(url_gtq, data=soap_request, headers=headers)
            response_tree = ET.fromstring(response.content)
            datagtq = {"soap": "http://schemas.xmlsoap.org/soap/envelope/", "ns": "http://www.banguat.gob.gt/variables/ws/"}
            value_rate_gtq = response_tree.find(".//ns:referencia", namespaces=datagtq).text
            valuation_date = response_tree.find(".//ns:fecha", namespaces=datagtq).text
            valuation_date = valuation_date.replace('/', '-')
            valuation_date = datetime.strptime(valuation_date, "%d-%m-%Y")
            logger.info('Se consulta la información necesaria del banco central de guatemala')
            return value_rate_gtq, valuation_date

        except (Exception,) as fpc_exc:
            raise_msg = "No se logro consultar el valor de USDGTQ"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc

    def create_dataframe_gtq(self,business_days_df,value_rate, effective_date):
        try:
            logger.info('Se inicia con la creacion del dataframa con la infomacion consultada...')
            dataframe_usd_gtq = pd.DataFrame()
            dataframe_usd_gtq['valuation_date'] = business_days_df['dates_calendar']
            dataframe_usd_gtq['value_rates'] = value_rate
            dataframe_usd_gtq['id_precia'] = 'USDGTQ'
            dataframe_usd_gtq['id_supplier'] = 'GTQ'
            dataframe_usd_gtq['effective_date'] = effective_date
            dataframe_usd_gtq = dataframe_usd_gtq[["id_precia","id_supplier", "value_rates", "valuation_date", "effective_date"]]
            logger.debug(dataframe_usd_gtq)
            logger.info('Se finaliza la creacion del dataframe.')
            return dataframe_usd_gtq
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro armar el dataframe con la informacion necesaria"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
    
class actions_db():
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
        
    
    def find_business_day(self):
        """
        Se realiza la consulta de los dias hábiles segun la fecha actual
        Args:
            next_day: siguiente día (hábil o no) de la fecha de valoración 
        Returns:
            business_days_df: dataframe con la información del día habil y los posteriores dias no habiles hasta el siquiente dia hábil
        """        
        
        try:
            db_connection = self.create_connection()
            next_day = valuation_date + timedelta(days=1)
            select_query = sa.sql.text(
            f"""SELECT dates_calendar, guatemala_calendar FROM precia_utils_calendars WHERE dates_calendar BETWEEN '{valuation_date}' AND 
            (SELECT MIN(dates_calendar) FROM precia_utils_calendars WHERE dates_calendar >= '{next_day}' AND guatemala_calendar =1)""")
            business_days_df = pd.read_sql(select_query, db_connection)
            business_days_df = business_days_df.iloc[:-1]
            logger.debug(business_days_df)
            db_connection.close()
            return business_days_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        
        
    def disable_previous_info(self, valuation_date):
        """
        Funcion encargada de deshabilitar la información anterior en la
        tabla, solo si se encuentra en la necesidad de volver a cargar la información
        """        
        error_msg = "No se pudo realizar la deshabilitacion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            conn = db_connection.connect()
            
            try:
                
                status_info = 0
                update_query = ("UPDATE src_exchange_rates SET status_info= {} WHERE id_precia = 'USDGTQ' and valuation_date = '{}'".format(status_info, valuation_date))
                logger.debug(update_query)
                conn.execute(update_query)
                
            except sa.exc.SQLAlchemyError as sql_exc:
                logger.error(create_log_msg(error_msg))
                raise_msg = (
                    "La base de datos no acepto los datos. validar tipos de datos"
                )
                raise PlataformError(raise_msg) from sql_exc
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
        

    def insert_data(self, info_df: pd.DataFrame):
        """
        Función encargada de realizar la inserción de la información a la base de datos  
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                logger.debug(info_df)
                if "index" in info_df.columns.values:
                    info_df = info_df.drop(["index"],axis=1,)
                info_df.to_sql('src_exchange_rates', con=db_connection, if_exists="append", index=False)
                logger.info(actions_db.CONN_CLOSE_ATTEMPT_MSG)
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

    

def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = boto3.client("lambda")
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


def get_parameter_from_ssm(parameter_name):
    """
    Obtiene un parámetro de Amazon Systems Manager (SSM) Parameter Store.

    Parámetros:
        parameter_name (str): El nombre del parámetro que se desea obtener.

    Retorna:
        str: El valor del parámetro almacenado en SSM Parameter Store.

    Excepciones:
        ssm_client.exceptions.ParameterNotFound: Si el parámetro no existe en SSM Parameter Store.
        Exception: Si ocurre un error inesperado al obtener el parámetro desde SSM Parameter Store.
    """
    ssm_client = boto3.client('ssm')
    try:
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        logger.info("Se obtuvo el parametro correctamente")
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        logger.error(f"El parámetro '{parameter_name}' no existe en Parameter Store.")
    except Exception as e:
        logger.error(f"Error al obtener el parámetro '{parameter_name}' desde r Parameter Store: {e}")


def update_report_process(status, description, technical_description):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """
    try:
        if isinstance(valuation_date, date):
            valuation_dates = valuation_date.strftime(date_format)
        else:
            valuation_dates = valuation_date
        lambda_name = get_parameter_from_ssm(parameter_store_name)
        report_process = {
            "input_id": "USDGTQ",
            "output_id":["USDGTQ"],
            "process": "rates",
            "product": "exchange-rates",
            "stage": "Recoleccion",
            "status": "",
            "aws_resource": "glue-p-rates-guatemala",
            "type": "insumo",
            "description": "",
            "technical_description": "",
            "valuation_date": valuation_dates
            }
        report_process["status"]=status
        report_process["description"]=description
        report_process["technical_description"]=technical_description
        launch_lambda(lambda_name, report_process)
        logger.info("Se envia el reporte de estado del proceso") 
    except (Exception,) as fpc_exc:
        raise_msg = "No se logro enviar el estado a la lambda de reportes"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError() from fpc_exc

try:
    params_key = [
        "DB_SECRET",
        "RATE_PARITY_LAMBDA",
        "VALUATION_DATE"
    ]
    params_dict = get_params(params_key)
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url = db_url_dict["conn_string"]
    schema_src = db_url_dict["schema_src_rates"]
    db_url_src = db_url + schema_src
    db_url_utils = db_url_dict["conn_string_aurora"]
    schema_utils = db_url_dict["schema_utils"]
    db_url_precia_utils = db_url_utils + schema_utils
    valuation_date_str = params_dict["VALUATION_DATE"]
    parity_lambda = params_dict["RATE_PARITY_LAMBDA"]
    parameter_store_name = 'ps-otc-lambda-reports'
    
    if valuation_date_str == "TODAY":
        colombia_offset = timedelta(hours=-5)
        colombia_timezone = timezone(colombia_offset)
        valuation_date = datetime.now(colombia_timezone).date()
    else:
        valuation_date = datetime.strptime(valuation_date_str, date_format)
        valuation_date = valuation_date.date() 
    logger.info(valuation_date)

    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc

def run():
    try:
        logger.info('Se inicia con la consulta al Web service del banco central de Guatemala')
        data_gtq = get_data()
        actionsdb = actions_db(db_url_precia_utils)
        business_days_df = actionsdb.find_business_day()
        value_rate, valuation_date = data_gtq.get_data_gtq()
        usdgtq_df = data_gtq.create_dataframe_gtq(business_days_df,value_rate, valuation_date)
        actionsdb = actions_db(db_url_src)
        business_days_df['dates_calendar'] = business_days_df['dates_calendar'].astype("string")
        dates = business_days_df['dates_calendar'].to_list()
        for date in dates:
            actionsdb.disable_previous_info(date)
        actionsdb.insert_data(usdgtq_df)  
        list_business_date = ' '.join([str(dates) for dates in dates])
        logger.debug(list_business_date)
        payload = {'valuation_date_list': list_business_date}
        launch_lambda(parity_lambda, payload)
        update_report_process("Exitoso", "Proceso Finalizado", "")
    except (Exception,) as init_exc:
        raise_msg = "No se ejecutar el Glue para la recoleccion de USDGTQ"
        logger.error(create_log_msg(raise_msg))
        update_report_process("Fallido", raise_msg, str(init_exc))
        raise PlataformError() from init_exc


if __name__ == "__main__":
    run()