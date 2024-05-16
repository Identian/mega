"""
Lambda que lanza el glue de publicación de curvas swap internacional
Glue: glue-p-otc-swp-inter-publish
Parámetros
--FILE_LIST: Nombre de los archivos a publicar
"""

import json
import json
import logging
import os
import re
import boto3
from datetime import datetime as dt

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def run_glue_job(name_glue_job: str, file_list: str):
    """Ejecuta el glue job
    """
    arguments = {
        "--FILE_LIST":file_list
    }
    glue = boto3.client('glue')
    logger.info("Comienza la ejecución del Glue")
    try:
        response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
    except (Exception,) as e:
        logger.error(f"Fallo la ejecucion del glue {name_glue_job}")
        
    job_run_id = response["JobRunId"]
    logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
    return job_run_id


def lambda_handler(event, context):
    logger.info("Inicia la ejecucion de la lambda ...")
    logger.info("Event:\n%s", json.dumps(event)) 
    
    name_glue_job = os.environ["JOB_NAME"]
    logger.info(f"Glue Job a ejecutar: {name_glue_job}")
    file_list =  event["FILE_LIST"]
    job_run_id = run_glue_job(name_glue_job, file_list)
    
    return {
        "statusCode": 200,
        "body": f"El Glue Job '{name_glue_job}' se lanzo bajo el id '{job_run_id}'"
    }