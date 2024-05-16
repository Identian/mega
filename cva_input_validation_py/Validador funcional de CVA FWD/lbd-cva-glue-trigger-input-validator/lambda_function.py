import json
import logging
import os
import boto3

from precia_logger import setup_logging, create_log_msg
from precia_exceptions import PlataformError

logger = setup_logging(logging.INFO)
MAX_FILE_SIZE = 5e7

def validate_file_structure(file_size: str) -> bool:
    """
    valida el tamanio del archivo

    Args:
        file_size (str): Tamanio del archivo insumo

    Raises:
        PlataformError: No supera la validacion de estructura

    Returns:
        bool: True si el archivo esta vacio
    """
    error_msg = "El archivo insumo no supero la validacion de estructura"
    empty_file = False
    try:
        logger.info("Validando la estructura del archivo ... ")
        file_size = int(file_size)
        if file_size > MAX_FILE_SIZE:
            logger.info("El archivo es sospechosamente grande, no sera procesado.")
            raise_msg = "El archivo supera el tamanio maximo aceptado. "
            raise_msg += f"Maximo: {MAX_FILE_SIZE}B. Informado: {file_size}B"
            raise PlataformError(raise_msg)
        if file_size == 0:
            logger.info("El archivo esta vacio.")
            empty_file = True
        logger.info("Validacion finalizada.")
        return empty_file
    except (Exception,) as vfs_exc:
        raise_msg = "Fallo la validacion del tamanio del archivo"
        error_msg = create_log_msg(raise_msg)
        logger.error(error_msg)
        raise PlataformError(raise_msg) from vfs_exc


def run_glue_job(name_glue_job, s3_full_path, s3_bucket):
    """
    Lanza una ejecucion del job de glue dado indicandole la ruta y el bucket
    del archivo que desandeno la ejecucion de esta lambda aws

    Args:
        name_glue_job (_type_): Nombre del job de glue a ejecutar
        s3_full_path (_type_): Ruta del archivo en el bucket de S3
        s3_bucket (_type_): Nombre del bucket de S3
    """
    try:
        glue = boto3.client("glue")
        arguments = {"--S3_CVA_PATH": s3_full_path, "--S3_CVA_BUCKET": s3_bucket}

        logger.info("Comienza la ejecución del Glue")
        response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
        job_run_id = response["JobRunId"]
        logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
    except (Exception,) as rgj_exc:
        raise_msg = f"Fallo el lanzamiento del job de glue {name_glue_job}"
        error_msg = create_log_msg(raise_msg)
        logger.error(error_msg)
        raise PlataformError(raise_msg) from rgj_exc


def lambda_handler(event, context):
    """Metodo Main de la lambda AWS

    Args:
        event (json): Evento recibido por la lambda cuando es desecandenada por
        la creacion de un archivo en el bucket s3-%env-cva-inputs  (%env:
        Ambiente: dev, qa o p)

    Returns:
        json: Respuesta de la lambda indicando si la ejecucion fue exitosa o
        fallida
    """    
    try:
        logger.info(f"Event: {json.dumps(event)}")
        file_size_s3 = event["Records"][0]["s3"]["object"]["size"]
        logger.debug("Tamanio del archivo: %s", file_size_s3)
        validate_file_structure(file_size_s3)
        s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        logger.info("Bucket trigger: %s", s3_bucket)
        s3_full_path = event["Records"][0]["s3"]["object"]["key"]
        logger.info("Ruta del archivo en el bucket: %s", s3_full_path)
        name_glue_job = os.environ["JOB_NAME"]
        logger.info(f"Glue Job a ejecutar: {name_glue_job}")
        run_glue_job(name_glue_job, s3_full_path, s3_bucket)

        return {"statusCode": 200, "body": json.dumps("Se realiza la ejecucion del glue")}
    except (Exception,):
        error_msg = create_log_msg("Ejecucion de lambda interrumpida")
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "body": json.dumps(error_msg),
        }
