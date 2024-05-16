#
# =============================================================
#
# Nombre: lbd-dev-layer-utils-schedules-status-get.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Obtiene el ultimo estado reportado en la tabla 'DB_TABLE' de los
diferentes procesos desarrollados durante la opreacion alrededor
deo Optimus K
"""
#
# Variables de entorno:
# DB_SCHEMA: precia_utils
# DB_TABLE: precia_utils_schedules_status
# SECRET_DB_NAME: precia/rds8/optimusk/utils
# SECRET_DB_REGION: us-east-1
#
# Requerimientos:
# capa-pandas-data-transfer
# capa-precia-utils
#
# =============================================================
#

from datetime import datetime, timedelta
import json
import logging
import sys

import pandas as pd
import precia_utils as utils


logger = logging.getLogger()
logger.setLevel(logging.INFO)
error_message = '' 
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info('[INIT] Inicializando Lambda ...')
    db_schema = utils.get_environment_variable('DB_SCHEMA')
    secret_db_region = utils.get_environment_variable('SECRET_DB_REGION')
    secret_db_name = utils.get_environment_variable('SECRET_DB_NAME')
    db_secret = utils.get_secret(secret_db_region, secret_db_name)
    db_table = utils.get_environment_variable('DB_TABLE')
    db_connection = utils.connect_to_db(db_schema, db_secret)
    logger.info('[INIT] Inicialización exitosa.')
except Exception as e:
    error_message = str(e)
    failed_init = True


def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en Lambas AWS
    :param event: JSON con los datos entregados a la Lambda por el desencadenador
    :param context: JSON con el contexto del Lambda suministrado por AWS
    :return: JSON con los datos solicitados o mensaje de error
    """
   
    now_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 

    if failed_init:  # Evita timeouts causados por errores en el codigo de inicializacion
        global error_message
        logger.critical('[lambda_handler] Funcion lambda interrumpida.')
        raise Exception(create_error_response(400, error_message, context))

    logger.debug('[lambda_handler] event: ' + str(json.dumps(event)))
    
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        clean_event = validate_event(event)
        processes_status = get_schedules_status(clean_event['schedule_list'])
        is_expected_status = validate_all_status(clean_event['expected_status'], processes_status)
        event_to_return = create_new_event(processes_status, is_expected_status, event)
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente ...")
        return event_to_return
    except UserError as e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise Exception(create_error_response(400, str(e), context))
    except Exception as  e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise Exception(create_error_response(500, str(e), context))


def validate_event(machine_event):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """

    error_msg = 'Event no supero la validacion.'
    try: 
        logger.info('[validate_event] Validando event ...')
        event_dict = machine_event['inputs']
        validated_event = {}
        validated_event['max_attempts'] = event_dict['max_attempts']
        validated_event['interval_seconds'] = event_dict['interval_seconds']
        validated_event['schedule_list'] = event_dict['schedule_list']
        for schedule in validated_event['schedule_list']:
            if type(schedule) is dict:
                byproduct_validated = bool(schedule['byproduct_id'])
                name_schedule_validated = bool(schedule['schedule_name'])
                type_schedule_validated = bool(schedule['schedule_type'])
        validated_event['expected_status'] = event_dict['expected_status']
        logger.info('[validate_event] El event tiene la estructura esperada. Validacion finalizada.')
        return validated_event
    except KeyError as e:
        logger.error('[validate_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise UserError(error_msg)
    except Exception as e:
        logger.error('[validate_event] Error inesperado al validar el event. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)
        

def get_schedules_status(schedule_list):
    """
    Ontiene los estados actuales de los procesos especificados en el 'schedule_list'
    """
    error_msg = 'No se concluyo la obtencion de los estados de los procesos'
    try:
        logger.info('[get_schedules_status] Obteniendo los estados de los procesos ...')
        
        today_date = (datetime.now() - timedelta(hours=5)).strftime('%Y-%m-%d') 
        schedules_list = []
        for schedule in schedule_list:
            byproduct_id = schedule['byproduct_id']
            schedule_name = schedule['schedule_name']
            schedule_type = schedule['schedule_type']
            query = 'SELECT id_byproduct, name_schedule, type_schedule, state_schedule, details_schedule, last_update FROM ' + db_table + ' WHERE '
            query += "id_byproduct = '{byproduct}' AND name_schedule = '{name}' AND type_schedule = '{type}' AND last_update >= '{today}'"
            query += 'ORDER BY last_update DESC LIMIT 1'
            query = query.format(byproduct=byproduct_id, name=schedule_name, type=schedule_type, today=today_date)
            logger.info('[get_schedules_status] schedule_query = ' + query)
            schedule_df = pd.read_sql(query, con=db_connection)
            if len(schedule_df) == 0:
                logger.warning('[get_schedules_status] schedule NO encontrado: ' + str(schedule))
                empty_schedule = dict()
                empty_schedule['id_byproduct'] = byproduct_id
                empty_schedule['name_schedule'] = schedule_name
                empty_schedule['type_schedule'] = schedule_type
                empty_schedule['state_schedule'] = 'No Ejecutado'
                empty_schedule['details_schedule'] = ''
                empty_schedule['last_update'] = ''
                schedule_df = pd.DataFrame([empty_schedule])
            else: 
                logger.info('[get_schedules_status] schedule encontrado: ' + str(schedule))
            schedules_list.append(schedule_df)
        schedules_df = pd.concat(schedules_list)
        schedules_df['last_update'] = schedules_df['last_update'].astype(str)
        logger.info('[get_schedules_status] Se obtuvieron los estados de los procesos de interes.')
        return schedules_df
    except Exception as e:
        logger.error('[get_schedules_status] ' + error_msg + '. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)


def validate_all_status(expected_status, all_schedules):
    """
    Valida que los estados de todos los procesos de 'all_schedules' sean igual a 'expected_status'
    """
    error_msg = 'Fallo validacion de los schedule solicitados'
    try:
        logger.info('[validate_all_status] Validando status de los schedules ...')
        logger.info('[validate_all_status] Status esperado: ' + expected_status)
        all_status = (all_schedules['state_schedule'] == expected_status).all()
        logger.info('[validate_all_status] Todos los schedules tiene el status esperado: ' + str(all_status))
        return bool(all_status)
    except Exception as e:
        logger.error('[validate_event] ' + error_msg + '. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)

        
def create_new_event(processes_status, is_expected_status, old_event):
    """
    Actualiza el event para la maquina estados con los ultimos estados consultados, con el resultado de la validacion
    'validate_all_status', si todos los procesos no tiene el estado esperado, actualiza el contador de reintentos
    """
    error_msg = "No se completo la creacion del 'body'"
    columns_dict = {
        "id_byproduct": "byproduct_id",
        "name_schedule": "schedule_name",
        "type_schedule": "schedule_type",
        "state_schedule": "schedule_state",
        "details_schedule": "schedule_details",
        "last_update": "update_date"
    }
    try:
        logger.info("[create_new_event] Creando 'body' para la respuesta ...")
        processes_status.rename(columns=columns_dict, inplace=True)
        processes_status_list = processes_status.to_dict('records')
        new_event = old_event
        new_event['inputs']['is_expected_status'] = is_expected_status
        new_event['inputs']['schedule_list'] = processes_status_list
        if 'current_attempts' in old_event['inputs']:
            new_event['inputs']['current_attempts'] = int(old_event['inputs']['current_attempts']) + 1
        else:
            new_event['inputs']['current_attempts'] = 1 
        logger.info("[create_new_event]'body' creado exitosamente")
        return new_event
    except Exception as e:
        logger.error('[create_new_event] ' + error_msg + '. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)


class UserError(Exception):
    """
    Clase heredada de Exception que permite etiquetar los errores causados por la informacion suministrada por el event
    """
    def __init__(self, error_message='Los datos suministrados por el event no tienen la estructura y/o valores esperados.'):
        self.error_message = error_message
        super().__init__(self.error_message)
    def __str__(self):
        return self.error_message


def get_error_line():
    """
    Obtiene el numero de linea de la ultima exception generada
    """
    return str(sys.exc_info()[-1].tb_lineno)


def create_error_response(status_code, error_message, context):
    """
    Crea la respuesta del API en caso de error, cumple el HTTP protocol version 1.1 Server Response Codes. Entre los
    valores que el diccionario que retorna se encuentra 'log_group', 'error_message' y 'request_id' que permite buscar
    el log en CloudWatch AWS
    :param status_code: Integer de codigo de respuesta del servidor 4XX o 5XX conforme al HTTP protocol version 1.1
    Server Response Codes
    :param error_type: String del tipo de error relacionado con 'status_code' conforme al HTTP protocol version 1.1
    Server Response Codes
    :param error_message: String con un mensaje en espaniol para que el usuario del API
    :param query_time: String de la fecha y hora que se ejecuto la solicitud
    :param context: Contexto del Lambda AWS
    :return: Diccionario con la respuesta lista para retornar al servicio API Gateway AWS
    """
    status_code_dict = {
        400: 'Bad Request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Not Allowed',
        500: 'Internal Server Error',
        501: 'Not Implemented',
        503: 'Service Unavailable',
        511: 'Network Authentication Required',
    }
    try:
        logger.debug('[precia_utils.create_error_response] Creando respuesta: error ...')
        error_response = {'statusCode': status_code}
        body = {'error_type': status_code_dict[status_code],
                'error_message': error_message}
        stack_trace = {'log_group': str(context.log_group_name),
                       'log_stream': str(context.log_stream_name),
                       'request_id': str(context.aws_request_id)}
        body['stack_trace'] = stack_trace
        error_response['body'] = body
        logger.debug('[precia_utils.create_error_response] Respuesta creada.')
        return error_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[precia_utils.create_error_response] No se pudo crear la respuesta: error. Fallo en linea: ' + error_line + '. Motivo: ' + str(
                e))