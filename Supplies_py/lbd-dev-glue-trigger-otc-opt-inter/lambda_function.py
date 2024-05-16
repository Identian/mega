"""
=============================================================
Nombre: lbd-dev-glue-trigger-otc-opt-inter
Tipo: Lambda AWS

Autores:
- Ruben Parra Medrano
- Hector Daza
Tecnología - Precia

Ultima modificación: 2022-11-17

Esta funcion lambda se desarrollo bajo el supuesto de ser
desencadenada por un evento de creacion de un objeto en un 
bucket y ruta configurados previamente, a partir de la 
informacion del desencadenador (event) se obtiene la ruta 
del nuevo archivo, la cual es pasada a un Glue Job como 
parametro usando boto3

=============================================================

"""

import boto3
from datetime import datetime
import json

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_response import create_error_response
from precia_utils.precia_aws import (
    get_config_secret,
    get_environment_variable,
)

ETL_FORMAT_DATE = "%Y-%m-%d"

try:
    logger = setup_logging()
    logger.info("Ejecutando incializacion...")
    CONFIG_SECRET_NAME = get_environment_variable("CONFIG_SECRET_NAME")
    CONFIG_SECRET = get_config_secret(CONFIG_SECRET_NAME)
    GLUE_JOB_NAME_KEY = get_environment_variable("GLUE_JOB_NAME_KEY")
    GLUE_JOB_NAME = CONFIG_SECRET[GLUE_JOB_NAME_KEY]
    GLUE_CLIENT = boto3.client("glue")
    VALUATION_DATE_CONFIG = CONFIG_SECRET["test/valuation_date"]
    try:
        datetime.strptime(VALUATION_DATE_CONFIG, ETL_FORMAT_DATE)
    except ValueError:
        VALUATION_DATE_CONFIG = datetime.now().strftime(ETL_FORMAT_DATE)
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
        context (obj): Objeto de AWS para almacenar informacion de la ejecucion de la lambda

    Raises:
        PlataformError: Cuando se presenta un error conocido en la ejecucion

    Returns:
        dict: Resultado de la ejecucion, si no es exitosa guia al usuario a llegar al log
    """
    error_msg = "Funcion lambda interrumpida"
    try:
        if FAILED_INIT:
            raise PlataformError("Fallo la inicializacion de la funcion lambda")
        logger.info("Inicia la ejecucion de la lambda ...")
        logger.info("Event:\n%s", json.dumps(event))
        s3_full_path = process_event(event)
        job_run_id = run_glue_job(GLUE_JOB_NAME, s3_full_path, context)
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


def process_event(trigger_event: dict) -> str:
    """
    Procesa el event del trigger para obtener el bucket que reporto el archivo nuevo
    (lo registra en el log) y la ruta de este nuevo archivo archivo

    Args:
        trigger_event (dict): event entregado por el desencadenador

    Raises:
        PlataformError: Cuando el event no tiene la estructura esperada de un
        desencadenador desde S3

    Returns:
        str: La ruta del archivo nuevo en el bucket S3 informado en el event
    """

    raise_msg = "Fallo el procesamiento del event"
    try:
        logger.info("Inicia el procesamiento del event")
        s3_bucket = trigger_event["Records"][0]["s3"]["bucket"]["name"]
        logger.info("Bucket trigger: %s", s3_bucket)
        s3_full_path = trigger_event["Records"][0]["s3"]["object"]["key"]
        logger.info("Ruta del archivo en el bucket: %s", s3_full_path)
        if VALUATION_DATE_CONFIG not in s3_full_path:
            logger.error(
                "El archivo insumo: %s no es para la fecha de valoracion configurada: %s",
                s3_full_path,
                VALUATION_DATE_CONFIG,
            )
            raise_msg = "No se lanza Glue Job de opciones por incompatibilidad "
            raise_msg += "con la fecha de valoracion configurada"
            raise PlataformError(raise_msg)
        logger.info("Procesamiento del event exitoso")
        return s3_full_path
    except KeyError as key_exc:
        logger.error(create_log_msg("El event no tiene la estructura esperada"))
        raise PlataformError(raise_msg) from key_exc
    except (Exception,):
        logger.error(create_log_msg("Error desconocido al procesar el event"))
        raise


def run_glue_job(glue_job_name: str, s3_full_path: str, context: object) -> str:
    """
    Utiliza la libreria boto3 para ejecutar o lanzar un AWS Glue Job. Le informa
    al Glue Job la ruta del bucket S3 del archivo a procesar a manera de parametro

    Args:
        glue_job_name (str): Nombre del Glue Job
        s3_full_path (str): Ruta del archivo a procesar
        context (): Objeto AWS con la metadata de la funcion lambda

    Raises:
        PlataformError: Cuando no es posible lanzar el Glue Job

    Returns:
        str: El id unico que identifica la ejecucion lanzada
    """
    try:
        logger.info("Lanzando el Glue Job '%s' ...", glue_job_name)
        function_arn = str(context.invoked_function_arn)
        arguments = {
            "--S3_FILE_PATH": s3_full_path,
            "--TRIGGER_RESOURCE_ARN": function_arn,
            "--JOB_NAME": glue_job_name
        }
        response = GLUE_CLIENT.start_job_run(JobName=glue_job_name, Arguments=arguments)
        job_run_id = response["JobRunId"]
        logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
        return job_run_id
    except (Exception,) as glue_exc:
        error_msg = f"No fue posible lanzar el Glue Job {glue_job_name}"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from glue_exc
