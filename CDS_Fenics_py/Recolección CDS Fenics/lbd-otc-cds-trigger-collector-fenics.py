"""
Lambda: Trigger Colector CDS
Esta lambda se encarga de ejecutar el Glue que recolecta el archivo de CDS
del vendor Fecnis
"""

import json
import logging
import os
import re
import boto3
from datetime import datetime as dt


logger = logging.getLogger()
logger.setLevel(logging.INFO)

    
def run_glue_job(name_glue_job, valuation_date):
    """Ejecuta el glue de recolección del archivo CDS
    
    Parameters:
        name_glue_jobs (str): Nombre del Glue Job a ejecutar
        valuation_date (str): Fecha para realizar la recolección
    
    Returns:
        None
    """
    arguments = {
        "--JOB_NAME": name_glue_job,
        "--VALUATION_DATE": valuation_date,
    }
    glue = boto3.client('glue')
    logger.info("Comienza la ejecución del Glue")
    response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
    job_run_id = response["JobRunId"]
    logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)


def lambda_handler(event, context):
    """
    Función principal de la lambda
    Parameters:
        event (dict): Evento recibido por la lambda.
        context (LambdaContext): Contexto de la lambda.

    Returns:
        dict: Respuesta de la lambda.
    """
    logger.info("Event:\n%s", json.dumps(event))
    valuation_date_str = event["valuation_date"]
    if valuation_date_str == "TODAY":
        today_date = dt.now().date()
        valuation_date_str = today_date.strftime("%d%m%y")
    name_glue_job = os.environ["JOB_NAME"]
    logger.info(f"Glue Job a ejecutar: {name_glue_job}")
    run_glue_job(name_glue_job, valuation_date_str)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Se lanzo el proceso de recoleccion del archivo de CDS proporcionado por Fenics')
    }