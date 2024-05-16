"""
Lambda: Trigger ETL for Bloomber-rates 
Esta lambda se encarga de ejecutar el Glue que extrae los datos del archivo
eviado por Bloomber para OTC CDS
"""

import json
import logging
import os
import re
import boto3
from datetime import datetime as dt


logger = logging.getLogger()
logger.setLevel(logging.INFO)
patron = r'\d{4}-\d{2}-\d{2}'

    
def run_glue_job(name_glue_job, bucket_name, file_name, valuation_date):
    """Ejecuta el glue de recolección del archivo Forwards
    
    Parameters:
        name_glue_jobs (str): Nombre del Glue Job a ejecutar
        valuation_date (str): Fecha para realizar la recolección
    
    Returns:
        None
    """
    arguments = {
        "--JOB_NAME": name_glue_job,
        "--BUCKET_NAME": bucket_name,
        "--FILE_NAME": file_name,
        "--VALUATION_DATE": valuation_date
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
   
    name_glue_job = os.environ["JOB_NAME"]
    logger.info(f"Glue Job a ejecutar: {name_glue_job}")
    
    # Get the bucket name and file key from the S3 event  
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    match_date = re.search(patron, file_key)
    valuation_date = match_date.group()
    
    run_glue_job(name_glue_job, bucket_name, file_key, valuation_date)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Se lanzo el proceso de recoleccion del archivo de CDS proporcionado por Bloomberg')
    }
