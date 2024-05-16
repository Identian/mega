
import json
import logging
import os
import boto3
from datetime import datetime as dt


logger = logging.getLogger()
logger.setLevel(logging.INFO)

    
def run_glue_job(name_glue_job):
    """Ejecuta el correspondiente el glue job de 
    hazzard local generacion archivos
    
    Parameters:
    -----------
    name_glue_job: str
        Nombre del Glue Job a ejecutar
    
    valuation_date: str
        Fecha del proceso
    
    """
    glue = boto3.client('glue')
    logger.info("Comienza la ejecución del Glue")
    response = glue.start_job_run(JobName=name_glue_job)
    job_run_id = response["JobRunId"]
    logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)


def lambda_handler(event, context):
    
    name_glue_job = os.environ["JOB_NAME"]
    logger.info(f"Glue Job a ejecutar: {name_glue_job}")
    run_glue_job(name_glue_job)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Se lanzo el proceso de generacion de archivos para el hazzard local')
    }