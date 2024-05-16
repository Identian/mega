import boto3
from datetime import datetime as dt
import json
import logging
import sys
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
MAX_FILE_SIZE = 5e7

ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")


#PRECIA_UTILS_EXCEPTIONS 
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


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

class MissingData(Exception):
    def __init__(
        self,
        error_message="Faltaron algunos tenores, ver el log para mas detalles",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)

#PRECIA_UTILS_LOGGER
def setup_logging():
    """
    formatea todos los logs que invocan la libreria logging
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = logging.StreamHandler(sys.stdout)
    precia_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
    logger.setLevel(logging.INFO)
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
    exception_type, exception_value, exception_traceback = sys.exc_info()
    if not exception_type:  # If exception_type == None
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )
    

FORMAT_DATE = "%Y-%m-%d"

try:
    logger = setup_logging()
    logger.info("Ejecutando incializacion...")
    GLUE_JOB_NAME = os.environ["JOB_NAME"]
    GLUE_CLIENT = boto3.client("glue")
    logger.info("Inicializacion exitosa")
except (Exception,):
    logger.error(create_log_msg("Fallo la inicializacion"))
    FAILED_INIT = True
else:
    FAILED_INIT = False


def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en una funcion lambda

    Args:
        event (dict): informacion suministradada por el desencadenador
        context (obj): Objeto de AWS que almacena informacion de la ejecucion
        de la lambda

    Raises:
        PlataformError: Cuando se presenta un error conocido en la ejecucion

    Returns:
        dict: Resultado de la ejecucion, si no es exitosa guia al usuario a
        llegar al log
    """
    error_msg = "Funcion lambda interrumpida"
    try:
        if FAILED_INIT:
            raise PlataformError("Fallo la inicializacion de la funcion lambda")
        logger.info("Inicia la ejecucion de la lambda ...")
        logger.info("Event:\n%s", json.dumps(event))
        valuation_date = extract_date(event=event)
        beta_data = extract_data(event=event)
        job_run_id = run_glue_job(
            glue_job_name=GLUE_JOB_NAME, valuation_date=valuation_date, beta_data=beta_data, context=context
        )
        
        logger.info("Finaliza exitosamente la ejecucion de la lambda")
        if job_run_id == 'null':
            return {
                "statusCode": 200,
                "body": "No fue posible lanzar el glue, debido a que no existe la data necesaria",
                }
        else:
            return {
                "statusCode": 200,
                "body": f"El Glue Job '{GLUE_JOB_NAME}' se lanzo bajo el id '{job_run_id}'",
            }
    except PlataformError as known_exc:
        logger.critical(create_log_msg(error_msg))
        logger.debug(known_exc)
        
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        msg_to_user = "Error desconocido, solicite soporte"
        logger.critical(create_log_msg(msg_to_user))
        

def run_glue_job(glue_job_name: str, valuation_date: list, beta_data: str, context: object) -> str:
    """Lanza una ejucucion del job de Glue indicado

    Args:
        glue_job_name (str): nombre del job de glue a lanzar
        valuation_date_list (list): fecha de valoracion
        context (object): Objeto de AWS que almacena informacion de la
        ejecucion de la lambda

    Raises:
        PlataformError: Cuando el intento de ejecucion del job de Glue falla

    Returns:
        str: ID de la ejecucion del job de Glue lanzada
    """    
    try:
        logger.info("Se intneta lanzar el Glue Job '%s' ...", glue_job_name)
        if beta_data == True:
            arguments = {
                "--JOB_NAME": glue_job_name,
                "--VALUATION_DATE": valuation_date,
            }
            response = GLUE_CLIENT.start_job_run(JobName=glue_job_name, Arguments=arguments)
            job_run_id = response["JobRunId"]
            logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
        else:
            job_run_id = 'null'
            logger.info('Aun no se ha recibido información sobre el archivo de los betas')
        return job_run_id
    except (Exception,) as glue_exc:
        error_msg = f"No fue posible lanzar el Glue Job {glue_job_name}"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from glue_exc


def extract_date(event: dict) -> str:
    """
    Extrae la fecha de valoracion del event necesaria como insumo para el glue
    de la muestra de ECB rates 

    Args:
        event (dict): event que recibe la funcion lambda

    Raises:
        PlataformError: Cuando el event no tiene la estructura esperada
        PlataformError: Cuando falla la extraccion de la fecha del event

    Returns:
        str: la fecha de valoracion contenida en el event
    """

    raise_msg = "Fallo el procesamiento del event"
    try:
        valuation_date_str = event["valuation_date"]
        logger.info(valuation_date_str)
        if valuation_date_str == "TODAY":
            today_date = dt.now().date()
            valuation_date_str = today_date.strftime(FORMAT_DATE)
        return valuation_date_str
    except KeyError as key_exc:
        logger.error(create_log_msg("El event no tiene la estructura esperada"))
        raise PlataformError(raise_msg) from key_exc
    except (Exception,) as ext_exc:
        raise_msg = "Fallo la extraccion de la fecha del event. "
        raise_msg += "Verifique si la fecha esta en el formato correcto"
        raise_msg += f": {FORMAT_DATE}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from ext_exc

def extract_data(event: dict):
    """
    Extrae la fecha de valoracion del event necesaria como insumo para el glue
    de la muestra de ECB rates 

    Args:
        event (dict): event que recibe la funcion lambda

    Raises:
        PlataformError: Cuando el event no tiene la estructura esperada
        PlataformError: Cuando falla la extraccion de la fecha del event

    Returns:
        str: la fecha de valoracion contenida en el event
    """

    raise_msg = "Fallo el procesamiento del event"
    try:
        sns_msg = False
        beta_data = event["files"]
        logger.info(beta_data)
        if 'SC' in beta_data: 
            sns_msg = True
        return sns_msg
    except KeyError as key_exc:
        logger.error(create_log_msg("El event no tiene la estructura esperada"))
        raise PlataformError(raise_msg) from key_exc
    except (Exception,) as ext_exc:
        raise_msg = "Fallo la extraccion de la fecha del event. "
        raise_msg += "Verifique si la fecha esta en el formato correcto"
        raise_msg += f": {FORMAT_DATE}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from ext_exc