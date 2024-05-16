import json
import logging
import os
import re
import boto3
from datetime import datetime as dt


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_date(event: dict) -> str:
    """Extrae la fecha de valoracion del event necesaria como insumo para el glue
    Parameters:
        event (dict): event que recibe la funcion lambda

    Returns:
        str: la fecha de valoracion contenida en el event
    """

    try:
        valuation_date_str = event["valuation_date"]
        if valuation_date_str == "TODAY":
            today_date = dt.now().date()
            valuation_date_str = today_date.strftime('%Y-%m-%d')
        return valuation_date_str
    except KeyError as key_exc:
        logger.error("El event no tiene la estructura esperada")
    

def run_glue_job(name_glue_job, valuation_date):
    """Ejecuta el correspondiente el glue job de 
    get calendars
    
    Parameters:
    -----------
    name_glue_job: str
        Nombre del Glue Job a ejecutar
    
    valuation_date: str
        Fecha del proceso
    
    """
    glue = boto3.client('glue')
    arguments = {
        "--VALUATION_DATE": valuation_date
        }
        
    logger.info("Comienza la ejecución del Glue")
    response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
    job_run_id = response["JobRunId"]
    logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)


def lambda_handler(event, context):
    
    logger.info(f"Event: {json.dumps(event)}")
    valuation_date = extract_date(event=event)
    name_glue_job = os.environ["JOB_NAME"]
    logger.info(f"Glue Job a ejecutar: {name_glue_job}")
    run_glue_job(name_glue_job, valuation_date)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Se lanzo el proceso para General Calendars correctamente')
    }