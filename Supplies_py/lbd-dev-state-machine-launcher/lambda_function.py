#
# =============================================================
#
# Nombre: lbd-dev-state-machine-launcher.py   
# Tipo: Lambda AWS
#
# Autores:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-01-30
#
"""
Lanza maquinas de estado con nombre de ejecucion personalizado,
ideal para usar en conjunto con Rules AWS
"""
#
# Variables de entorno:
#
# Requerimientos:
#
# =============================================================
#

from datetime import datetime
from dateutil import relativedelta as rd
import boto3
import json

import requests

from precia_utils.precia_aws import get_config_secret, get_environment_variable
from precia_utils.precia_exceptions import PlataformError, UserError
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_response import create_error_response

DATE_FORMAT = "%Y-%m-%d"

try:
    logger = setup_logging()
    logger.info("Inicializando Lambda ...")
    CONFIG_SECRET_NAME = get_environment_variable("CONFIG_SECRET_NAME")
    CONFIG_SECRET = get_config_secret(CONFIG_SECRET_NAME)
    MAIL_TO = CONFIG_SECRET["validator/mail_to"].replace(" ", "")
    MAIL_TO = MAIL_TO.split(",")
    PRECIA_API = CONFIG_SECRET["url/api_precia"]
    API_TIMEOUT = int(get_environment_variable('API_TIMEOUT'))
    VALUATION_DATE = CONFIG_SECRET["test/valuation_date"]
    try:
        valuation_date = datetime.strptime(VALUATION_DATE, DATE_FORMAT)
    except ValueError:
        today_date = datetime.now() - rd.relativedelta(hours=5)
        VALUATION_DATE = datetime.strftime(today_date, DATE_FORMAT)
    client_step_functions = boto3.client("stepfunctions")
    logger.info("Inicialización exitosa.")
except (Exception,):
    logger.error(create_log_msg("Fallo la inicializacion"))
    FAILED_INIT = True
else:
    FAILED_INIT = False


def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en Lambas AWS
    :param event: JSON con los datos entregados a la Lambda por desencadenador
    :param context: JSON con el contexto del Lambda suministrado por AWS
    :return: JSON con los datos solicitados o mensaje de error
    """
    log_msg = "Lambda interrumpida"
    try:
        if FAILED_INIT:
            raise PlataformError("Fallo la inicializacion")
        logger.info("Ejecutando funcion Lambda ...")
        is_consulted_in_holiday = event['state_machine_input']['collector_input']['product'] in ['swp_inter','fwd_inter']
        logger.info('Debe consultarse en dias no habiles?: %s',is_consulted_in_holiday)
        #Swap inter y fwd inter debe correr de lunes a viernes siempre
        if not (check_business_day(VALUATION_DATE) or is_consulted_in_holiday):
            raise PlataformError(f"{VALUATION_DATE} NO es dia habil")
        now_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info("event: %s", json.dumps(event))
        clean_event = validate_event(event)
        execution_name = launch_state_machine(clean_event, now_time_str)
        body = f"La ejecucion {execution_name} lanzada."
        logger.info(body)
        logger.info("Funcion Lambda ejecutada exitosamente.")
        return {"statusCode": 200, "body": body}
    except PlataformError as plat_exc:
        logger.critical(create_log_msg(log_msg))
        response_msg = str(plat_exc)
        return create_error_response(500, response_msg, context)
    except UserError:
        logger.critical(create_log_msg(log_msg))
        response_msg = "Error en el event del trigger"
        return create_error_response(400, response_msg, context)
    except (Exception,):
        logger.critical(create_log_msg(log_msg))
        response_msg = "Error inesperado. Solicite soporte."
        return create_error_response(500, response_msg, context)


def validate_event(lambda_event):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """
    log_msg = "El event no tiene el formato esperado"
    try:
        logger.info("Validando el event ...")
        validated_event = {}
        state_machine_key = lambda_event["state_machine_arn"]
        validated_event["state_machine_arn"] = CONFIG_SECRET[state_machine_key]
        validated_event["state_machine_excution"] = lambda_event[
            "state_machine_excution"
        ]
        logger.info("State machine : %s", validated_event["state_machine_arn"])
        validated_event["state_machine_input"] = lambda_event["state_machine_input"]
        validated_event["state_machine_input"]["valuation_date"] = VALUATION_DATE
        if "collector_arn" in json.dumps(validated_event):
            logger.info("TIPO: Colector")
            arn_key = validated_event["state_machine_input"]["collector_arn"]
            collector_arn = CONFIG_SECRET[arn_key]
            validated_event["state_machine_input"]["collector_arn"] = collector_arn
            validated_event["state_machine_input"]["email_to_notify"]["body"][
                "recipients"
            ] = MAIL_TO
        else:
            logger.info("TIPO: Proceso")
            validated_event["state_machine_input"]["error_email"][
                "recipients"
            ] = MAIL_TO
            validated_event["state_machine_input"]["precia_api"][
                "endpoint"
            ] = PRECIA_API
        logger.info("Event correcto.")
        return validated_event
    except KeyError as key_exc:
        logger.error(create_log_msg(log_msg))
        raise UserError("event incompleto") from key_exc
    except (Exception,):
        logger.error(create_log_msg(log_msg))
        raise


def launch_state_machine(event_dict, now_time_str):
    """
    Lanza la maquina de estados 'state_machine_arn' con el json input
    'state_machine_input' actualizando el nombre de la
    ejecuion 'state_machine_excution' agregandole 'now_time_str'
    """
    log_msg = "No fue posible ejecutar la maquina solicitada"
    try:
        logger.info("Lanzando state machine ...")
        state_machine_process_arn = event_dict["state_machine_arn"]
        logger.info("state_machine_arn: %s", state_machine_process_arn)
        execution_base_name = event_dict["state_machine_excution"]
        input_dict = event_dict["state_machine_input"]
        execution_name = (
            execution_base_name + "_" + now_time_str.replace(":", ".").replace(" ", "_")
        )
        client_step_functions.start_execution(
            stateMachineArn=state_machine_process_arn,
            name=execution_name,
            input=json.dumps(input_dict),
        )
        logger.info("Maquina ejecutandose bajo el id: %s", execution_name)
        return execution_name
    except (Exception,):
        logger.error(create_log_msg(log_msg))
        raise


def check_business_day(valuation_date_str: str) -> bool:
    """
    Revisa que el dia de valoracion sea un dia habil

    Args:
        valuation_date_str (str): Fecha habil configurada en el secreto de configuracion

    Raises:
        PlataformError: Cuando la API de calendarios de Precia no responde satisfactoriamente

    Returns:
        bool: True si es dia habil y False si NO es dia habil
    """
    error_log = "No fue posible verificar si es dia habil"
    try:
        logger.info("Verificando si la fecha de valoracion es un dia habil ...")
        run_date = datetime.strptime(valuation_date_str, DATE_FORMAT)
        init_date = run_date - rd.relativedelta(days=3)
        end_date = run_date + rd.relativedelta(days=3)
        init_date_str = datetime.strftime(init_date, DATE_FORMAT)
        end_date_str = datetime.strftime(end_date, DATE_FORMAT)
        calendar_url = f"{PRECIA_API}/utils/calendars?"
        calendar_url += f"campos=CO&fecha-inicial={init_date_str}&fecha-final={end_date_str}"
        logger.info("URL API: %s", calendar_url)
        api_response = requests.get(calendar_url, timeout=API_TIMEOUT)
        api_response_code = api_response.status_code
        api_response_data = api_response.json()
        api_response_msg = (
            f"code: {api_response_code}, body: {api_response_data}."
        )
        logger.info("Repuesta del API: %s", api_response_msg)
        if api_response_code != 200:
            log_msg = " La respuesta del API contiene un codigo de error"
            logger.error(create_log_msg(log_msg))
            raise_msg = (
                "El API de calendarios no respondio satisfactoriamente a la solicitud"
            )
            raise PlataformError(raise_msg)
        calendar_data = api_response_data["data"]
        if valuation_date_str not in calendar_data:
            log_msg = f"La fecha de valoracion {valuation_date_str} NO es un dia habil"
            logger.error(create_log_msg(log_msg))
            return False
        log_msg = f"La fecha de valoracion {valuation_date_str} es un dia habil"
        logger.info(log_msg)
        return True
    except (Exception,):
        logger.error(create_log_msg(error_log))
        raise
