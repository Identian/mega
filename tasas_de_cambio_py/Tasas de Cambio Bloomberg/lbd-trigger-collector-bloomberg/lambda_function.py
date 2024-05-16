"""
Lambda: Trigger Colector rates bloomberg
Esta lambda se encarga de ejecutar el Glue que recolecta tasas del vendor bloomberg
"""

import json
import logging
import os
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def run_glue_job(name_glue_job, event):
    """Ejecuta el glue de recolección del archivo Forwards

    Parameters:
        name_glue_jobs (str): Nombre del Glue Job a ejecutar
        event (dict): argumentos para el glue

    Returns:
        None
    """
    bucket_name = event["destination"]["bucket"]
    bucket_path = event["destination"]["path"]
    full_bucket_path = os.path.join(bucket_name, bucket_path)
    arguments = {
        "--JOB_NAME": name_glue_job,
        "--INSTRUMENT": event["instrument"],
        "--OUTPUT_BUCKET_URI": f"s3://{full_bucket_path}",
        "--OUTPUT_ID": ",".join(event["output_id"]),
        "--PROCESS": event["process"],
        "--PRODUCT": event["product"],
        "--REQUEST_FIELDS": ",".join(event["requests"]["fields"]),
        "--REQUEST_IDENTIFIERS": ",".join(event["requests"]["identifiers"]),
        "--REQUEST_TEMPLATE": event["requests"]["template"],
    }
    glue = boto3.client("glue")
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
    logger.info("Glue Job a ejecutar: %s", name_glue_job)
    run_glue_job(name_glue_job, event)

    return {
        "statusCode": 200,
        "body": json.dumps(
            "Se lanzo el proceso de recoleccion de rates desde bloomberg"
        ),
    }
