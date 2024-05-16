import boto3
from datetime import datetime as dt
import json
import sys
import logging
import os


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



    
ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)

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
      


def get_environment_variable(variable_key: str) -> str:
    """
    Obtiene la variable de entorno bajo la llave 'variable_key', sino
    lo encuentra lanza un exception

    Args:
        variable_key (str): Nombre de la variable de entorno configurada
        en la lambda

    Raises:
        PlataformError: Cuando no encuentra ninguna variable bajo el nombre
        declarado en 'variable_key'

    Returns:
        str: String con el valor relacionado con la llave 'variable_key'
    """
    try:
        logger.info('Obteniendo variables de entorno "%s" ...', variable_key)
        variable_value = os.environ[str(variable_key)]
        logger.info("Variable obtenida con exito.")
        return variable_value
    except Exception as var_exc:
        logger.error(create_log_msg("No se encontro variable de entorno"))
        raise PlataformError("Variable de entorno no encontrada") from var_exc


FORMAT_DATE = "%Y-%m-%d"

try:
    logger = setup_logging()
    logger.info("Ejecutando incializacion...")
    GLUE_JOB_NAME = get_environment_variable("JOB_NAME")
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
        valuation_date_list = extract_date(event=event)
        
        job_run_id = run_glue_job(
            glue_job_name=GLUE_JOB_NAME, valuation_date_list=valuation_date_list, context=context)
        logger.info(valuation_date_list)
        logger.info("Finaliza exitosamente la ejecucion de la lambda")
        return {
            "statusCode": 200,
            "body": f"El Glue Job '{GLUE_JOB_NAME}' se lanzo bajo el id '{job_run_id}'",
        }
    except PlataformError as unknown_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from unknown_exc  
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        msg_to_user = "Error desconocido, solicite soporte"
        logger.error(create_log_msg(msg_to_user))
        


def run_glue_job(glue_job_name: str, valuation_date_list: list, context: object) -> str:
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
        logger.info("Lanzando el Glue Job '%s' ...", glue_job_name)
        arguments = {
            "--JOB_NAME": glue_job_name,
            "--VALUATION_DATE": valuation_date_list
        }
        response = GLUE_CLIENT.start_job_run(JobName=glue_job_name, Arguments=arguments)
        job_run_id = response["JobRunId"]
        logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
        return job_run_id
    except (Exception,) as glue_exc:
        error_msg = f"No fue posible lanzar el Glue Job {glue_job_name}"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from glue_exc


def extract_date(event: dict) -> str:
    """
    Extrae la fecha de valoracion del event necesaria como insumo para el glue
    de la muestra de fwd inter

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
        valuation_date_list_str = event["valuation_date"]
        valuation_date_str = str(valuation_date_list_str[0])
        if valuation_date_str == "TODAY":
            valuation_date_list_str = dt.now().strftime(FORMAT_DATE)
            logger.debug(valuation_date_list_str)
        else:
            valuation_date_list_str = dt.strptime(valuation_date_list_str, FORMAT_DATE)
            valuation_date_list_str = valuation_date_list_str.strftime(FORMAT_DATE)
        return valuation_date_list_str
    except KeyError as key_exc:
        logger.error(create_log_msg("El event no tiene la estructura esperada"))
        raise PlataformError(raise_msg) from key_exc
    except (Exception,) as ext_exc:
        raise_msg = "Fallo la extraccion de la fecha del event. "
        raise_msg += "Verifique si la fecha esta en el formato correcto"
        raise_msg += f": {FORMAT_DATE}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from ext_exc
    
