import json
import logging
import os
import boto3
from datetime import datetime as dt

from precia_logger import setup_logging, create_log_msg
from precia_exceptions import PlataformError

logger = setup_logging(logging.INFO)


def extract_date(event: dict) -> str:
    """Extrae la fecha de valoracion del event necesaria como insumo para el glue"""

    try:
        valuation_date_str = event["valuation_date"][0]
        if valuation_date_str == "TODAY":
            today_date = dt.now().date()
            valuation_date_str = today_date.strftime("%Y-%m-%d")
        return valuation_date_str
    except KeyError as ext_exc:
        raise_msg = "El event no tiene la estructura esperada"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from ext_exc
    except (Exception,) as ext_exc:
        raise_msg = "Fallo la extraccion de la fecha"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from ext_exc


def run_glue_job(name_glue_job, valuation_date, swap_curve):
    """Ejecuta el glue de proceso de swap inter con la fecha de valoracion
    y el nombre de la curva
    """
    try:
        glue = boto3.client("glue")
        arguments = {"--VALUATION_DATE": valuation_date, "--SWAP_CURVE": swap_curve}
        logger.info("Comienza la ejecución del Glue")
        response = glue.start_job_run(JobName=name_glue_job, Arguments=arguments)
        job_run_id = response["JobRunId"]
        logger.info("El Glue Job se lanzo bajo el id: %s.", job_run_id)
    except (Exception,) as rgj_exc:
        raise_msg = "Fallo en lanzamiento de la ejecucion del job de glue %s"
        logger.error(create_log_msg(raise_msg), name_glue_job)
        raise PlataformError(raise_msg) from rgj_exc


def lambda_handler(event, context):
    try:
        logger.info(f"Event: {json.dumps(event)}")
        valuation_date = extract_date(event=event)
        swap_curve = event["input_name"][0]
        name_glue_job = os.environ["JOB_NAME"]
        logger.info(f"Glue Job a ejecutar: {name_glue_job}")
        run_glue_job(name_glue_job, valuation_date, swap_curve)

        return {
            "statusCode": 200,
            "body": json.dumps(
                "Se lanzo el proceso de generacion de las curvas eonia y eurois (estr)  correctamente"
            ),
        }
    except (Exception,):
        logger.error(create_log_msg("Ejecucion de lambda interrumpida"))
        return {
            "statusCode": 500,
            "body": json.dumps("Fallo la ejecucion de la lambda"),
        }
