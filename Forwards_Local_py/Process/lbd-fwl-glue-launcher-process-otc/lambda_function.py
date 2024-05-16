"""
=============================================================
Nombre: lbd-dev-fwl-glue-launcher-process-otc

Tipo: Lambda AWS

Autores:
- Hector Daza
Tecnología - Precia

Ultima modificación: 2023-02-01

Esta lambda lanza ejucta un job de Glue psandole los siguientes parametros:
    "--TRIGGER_RESOURCE_ARN": function_arn, --> arn de esta lambda
    "--JOB_NAME": glue_job_name,
    "--VALUATION_DATE": valuation_date,

=============================================================

"""

import boto3
from datetime import datetime as dt
import json

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_response import create_error_response
from precia_utils.precia_aws import (
    get_environment_variable,
)

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
        valuation_date = extract_date(event=event)
        job_run_id = run_glue_job(
            glue_job_name=GLUE_JOB_NAME, valuation_date=valuation_date, context=context
        )
        logger.info("Finaliza exitosamente la ejecucion de la lambda")
        return {
            "statusCode": 200,
            "body": f"El Glue Job '{GLUE_JOB_NAME}' se lanzo bajo el id '{job_run_id}'",
        }
    except PlataformError as known_exc:
        logger.critical(create_log_msg(error_msg))
        error_response = create_error_response(500, str(known_exc), context)
        return error_response
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        msg_to_user = "Error desconocido, solicite soporte"
        error_response = create_error_response(500, msg_to_user, context)
        return error_response


def run_glue_job(glue_job_name: str, valuation_date: str, context: object) -> str:
    """Lanza una ejucucion del job de Glue indicado

    Args:
        glue_job_name (str): nombre del job de glue a lanzar
        valuation_date (str): fecha de valoracion
        context (object): Objeto de AWS que almacena informacion de la
        ejecucion de la lambda

    Raises:
        PlataformError: Cuando el intento de ejecucion del job de Glue falla

    Returns:
        str: ID de la ejecucion del job de Glue lanzada
    """    
    try:
        logger.info("Lanzando el Glue Job '%s' ...", glue_job_name)
        function_arn = str(context.invoked_function_arn)
        arguments = {
            "--TRIGGER_RESOURCE_ARN": function_arn,
            "--JOB_NAME": glue_job_name,
            "--VALUATION_DATE": valuation_date,
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
    de la muestra de fwd local

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
