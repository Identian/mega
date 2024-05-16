import json
import logging
import os
import re
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
MAX_FILE_SIZE = 5e7


    
    
def validate_file_structure(file_size: str) -> bool:
    """
    valida el archivo desde el peso del mismo

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
            logger.info(
                "El archivo %s es sospechosamente grande, no sera procesado.", FILE_NAME
            )
            raise_msg = f"El archivo {FILE_NAME} supera el tamanio maximo aceptado. "
            raise_msg += "Maximo: {MAX_FILE_SIZE}B. Informado: {file_size}B"
            raise PlataformError(raise_msg)
        if file_size == 0:
            logger.info("El archivo %s esta vacio.", FILE_NAME)
            empty_file = True
        logger.info("Validacion finalizada.")
        return empty_file
    except PlataformError:
        logger.critical(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise
    

def run_glue_job(name_glue_job, s3_full_path, s3_bucket):
    """Ejecuta el correspondiente el glue job correspondiente
    al archivo recibido desde S3
    
    Parameters:
    -----------
    name_glue_job: str
        Nombre del Glue Job a ejecutar
    
    s3_full_path: str
        Ruta del archivo en S3 para colocarla en los parámetros del Glue
    
    """
    glue = boto3.client('glue')
    arguments = {
        "--S3_FILE_PATH": s3_full_path,
        "--S3_FWI_BUCKET":s3_bucket
        }
        
    logger.info("Comienza la ejecución del Glue")
    response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
    job_run_id = response["JobRunId"]
    logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)


def lambda_handler(event, context):
    
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
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Se realiza la ejecucion del glue')
    }