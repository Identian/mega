"""
Lanza el glue trigger para cross, enviandole los siguientes datos
--PRODUCT: Nombre del producto que se reporta
--INPUT_NAME: Proceso que se reporta
--VALUATION_DATE: fecha del proceso

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

def run_glue_job(name_glue_job: str, event:dict):
    """Ejecuta el glue job que gestiona el trigger de cross
    """
    arguments = {
        "--PRODUCT": event["product"],
        "--INPUT_NAME": ",".join(event["input_name"]),
        "--VALUATION_DATE": ",".join(event["valuation_date"])
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
    job_run_id = run_glue_job(name_glue_job, event)
    return {
        "statusCode": 200,
        "body": f"El Glue Job '{name_glue_job}' se lanzo bajo el id '{job_run_id}'"
    }
