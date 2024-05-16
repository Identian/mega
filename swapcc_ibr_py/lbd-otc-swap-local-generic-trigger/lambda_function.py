"""
Lambda que lanza el glue de metodología para la curva
SwapCC_IBR
Glue: glue-p-swl-etl-ibr-cc
Parámetros
--SWAP_CURVE: Nombre de la curva a procesar
--VALUATION_DATE: Fecha del proceso
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

def run_glue_job(name_glue_job: str, swap_name: str, valuation_date: str):
    """Ejecuta el glue job que gestiona el trigger de cross
    """
    arguments = {
        "--SWAP_CURVE": swap_name,
        "--VALUATION_DATE": valuation_date
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
    if event["swap_curve"] == "IBR":
        logger.info("Se ejecuta el proceso de la curva SwapCC_IBR")
        swap_name = "SwapCC_"+event["swap_curve"]
        job_run_id = run_glue_job(name_glue_job, swap_name, event["valuation_date"])

    
    return {
        "statusCode": 200,
        "body": f"El Glue Job '{name_glue_job}' se lanzo bajo el id '{job_run_id}'"
    }