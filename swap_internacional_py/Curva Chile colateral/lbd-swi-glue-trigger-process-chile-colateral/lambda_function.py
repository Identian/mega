"""
Desencadena la ejecucion del job de glue parametrizado en la variable de
entorno "JOB_NAME". Agrega el parametro "--VALUATION_DATE" a la ejecucion del
job de glue en formato "%Y-%m-%d"; el string que coloca en el paremetro lo
obtiene del event:
    Ejemplo de event:
        {
            "valuation_date":["2023-12-31"]
        }
    o tambien:
        {
            "valuation_date":"2023-12-31"
        }
Si la fecha no se especifica en el event, o si el formato del event no es el
esperado, obtiene la fecha de ejecución y la usa en reemplazo de la fecha
esperada
"""

import json
import logging
import os
from datetime import datetime as dt
from sys import exc_info, stdout
from boto3 import client as aws_client

# LOGGER: INICIO --------------------------------------------------------------
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


logger = setup_logging(logging.INFO)

# LOGGER: FIN -----------------------------------------------------------------


# EXCEPCIONES PERSONALIZADAS: INICIO--------------------------------------------


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


# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------


def extract_date(event: dict) -> str:
    """Extrae la fecha de valoracion del event necesaria como insumo para el glue"""
    try:
        if isinstance(event["valuation_date"],list):
            valuation_date_str = event["valuation_date"][0]
        else:
            valuation_date_str = event["valuation_date"]
        dt.strptime(
            valuation_date_str, "%Y-%m-%d"
        )  # Solo para verificar que es una fecha valida
        return valuation_date_str
    except (Exception,):
        raise_msg = "Fallo la extraccion de la fecha del event. Se tomara la fecha de hoy en su lugar"
        logger.warning(create_log_msg(raise_msg))
        today_date = dt.now().date()
        valuation_date_str = today_date.strftime("%Y-%m-%d")
        return valuation_date_str


def run_glue_job(name_glue_job, valuation_date):
    """Ejecuta el job de glue indicado pasandole la fecha dada"""
    try:
        glue = aws_client("glue")
        arguments = {"--VALUATION_DATE": valuation_date}
        logger.info("Comienza la ejecución del Glue")
        response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
        job_run_id = response["JobRunId"]
        logger.info("El Glue Job se lanzo bajo el id: %s", job_run_id)
        return job_run_id
    except (Exception,) as gen_exc:
        raise_msg = (
            f"Fallo en lanzamiento de la ejecucion del job de glue: {name_glue_job}"
        )
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from gen_exc





def lambda_handler(event, context): 

    try:
        logger.info(f"Event: {json.dumps(event)}")
        event = {k.lower(): v for k, v in event.items()}
        valuation_date = extract_date(event)
        logger.info("valuation_date: %s", valuation_date)
        name_glue_job = os.environ["JOB_NAME"] 

        logger.info(f"Glue Job a ejecutar: {name_glue_job}")
        
        job_run_id = run_glue_job(
            name_glue_job=name_glue_job, valuation_date=valuation_date
        )

        response = {
            "statusCode": 200,
            "body": f"Se lanzo el job de glue {name_glue_job} ({job_run_id}) con --VALUATION_DATE = {valuation_date} correctamente!!!",
        }
        logger.info("Lambda response:\n%s", json.dumps(response))
        return response 
    except (Exception,):
        error_msg = create_log_msg("Ejecucion de lambda interrumpida")
        logger.error(error_msg)
        response = {
            "statusCode": 500,
            "body": "Fallo la ejecucion de la lambda",
            "error": error_msg,
        }
        logger.error("Lambda response:\n%s", json.dumps(response))
        return response  
