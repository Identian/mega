#
# =============================================================
#
# Nombre: lbd-dev-data-history-builder.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
A partir de la informacion validada del event crea un historico
al consultar un endpoint repetidas veces solo variando un
parametro URL, actualmente solo tipo fecha. El intervalo del
historico puede ser definido a partir de las fechas inicial y
final, o la declaracion de la fecha incial y un intervalo.
"""
#
# Variables de entorno:
# PAGINATION_MAX_LIMIT: '1000'
#
# Requerimientos:
# capa-pandas-requests
#
# =============================================================
#

from datetime import datetime, timedelta
from dateutil import relativedelta as rd
import json
import logging
import os
import sys

import requests

logger = logging.getLogger()  # Log, ver en CloaudWatch
logger.setLevel(logging.INFO)
error_message = ''  # Mensaje de error para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info('[INIT] Inicializando Lambda ...')
    max_total_results = int(os.environ['PAGINATION_MAX_LIMIT'])
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
    if failed_init:  # Evita timeouts causados por errores en el codigo de inicializacion
        logger.critical('[lambda_handler] Lambda interrumpida. No se completo la inicializacion.')
        global error_message
        return create_error_response(500, error_message, context)

    logger.info('[lambda_handler] event: ' + str(json.dumps(event)))
    
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        base_historical_url, historical_date_list, time_period_str, query_date, end_point = process_event(event)
        historical_data_dict = get_historical_data(base_historical_url, historical_date_list)
        body_to_return_dict = create_body(historical_data_dict, time_period_str, query_date, end_point)
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente ...")
        return {
            'statusCode': 200,
            'body': body_to_return_dict
        }
    except UserError as e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise Exception(create_error_response(400, str(e), context))
    except Exception as  e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise Exception(create_error_response(500, str(e), context))


def process_event(event_lambda):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """
    error_msg = 'Fallo el procesamiento del event'
    try:
        logger.info('[process_event] Iniciando procesamiento del event ...')
        end_point = event_lambda['private_api_url']
        url = end_point.replace(' ', '') + '?'
        ulr_params_dict = event_lambda['url_params']
        ulr_params_list = list(ulr_params_dict)
        if len(ulr_params_list) == 0:
            raise UserError("Event no incluye 'url_params' para generar el historico")
        if not 'historical_url_param' in ulr_params_list:
            raise UserError("El event no incluye ['url_params']['historical_url_param'] para generar el historico")
        historical_url_param_name = ulr_params_dict['historical_url_param']['url_param_name']
        if not bool(ulr_params_dict['historical_url_param']['initial_date']):
            raise UserError("El event debe incluir [...]['url_params']['initial_date'] valido para generar el historico")
        initial_date_str = ulr_params_dict['historical_url_param']['initial_date']
        if initial_date_str == '<<<TODAY>>>':
            historical_initial_date = datetime.now() - timedelta(hours=5)
        elif initial_date_str == '<<<YESTERDAY>>>':
            historical_initial_date = datetime.now() - timedelta(hours=5,days=1)
        else:
            try:
                historical_initial_date = datetime.strptime(initial_date_str, '%Y-%m-%d')
            except Exception as e:
                raise ValueError('"initial_date" no tiene el formato esperado AAAA-MM-DD.')
        historical_final_date = False
        final_date = False
        add_time_period = False
        subtract_time_period = False
        if 'final_date' in ulr_params_dict['historical_url_param'] and bool(ulr_params_dict['historical_url_param']['final_date']):
            try:
                final_date = datetime.strptime(ulr_params_dict['historical_url_param']['final_date'], '%Y-%m-%d')
            except Exception as e:
                raise ValueError('"final_date" no tiene el formato esperado AAAA-MM-DD.')
        if 'add_time_period' in ulr_params_dict['historical_url_param'] and bool(ulr_params_dict['historical_url_param']['add_time_period']):
            historical_time_period = ulr_params_dict['historical_url_param']['add_time_period']
            add_time_period = create_deltatime(historical_time_period)
        if 'subtract_time_period' in ulr_params_dict['historical_url_param'] and bool(ulr_params_dict['historical_url_param']['subtract_time_period']):
            historical_time_period = ulr_params_dict['historical_url_param']['subtract_time_period']
            subtract_time_period = create_deltatime(historical_time_period)
        if (bool(add_time_period) or bool(subtract_time_period)) == bool(historical_final_date) and bool(historical_final_date):
            raise UserError("Solo incluir uno: event[...]['historical_url_param']['*_time_period'] o event[...]['historical_url_param']['final_date']")
        initial_date = historical_initial_date
        if bool(add_time_period):
            final_date = historical_initial_date + add_time_period
        if bool(subtract_time_period):
            initial_date = historical_initial_date - subtract_time_period
        if not bool(final_date):
            final_date = historical_initial_date
        if final_date < initial_date:
            raise UserError("event[...]['historical_url_param']['initial_date'] debe ser menor a event[...]['historical_url_param']['final_date']")
        time_period_days = (final_date - initial_date).days
        time_period_date_list = []
        for days in range(time_period_days + 1):
            date_to_historical_data = (initial_date + rd.relativedelta(days=days)).strftime("%Y-%m-%d")
            time_period_date_list.append(date_to_historical_data)
        ulr_params_list.remove('historical_url_param')
        for parameter in ulr_params_list:
            url += str(parameter) + '=' + str(ulr_params_dict[parameter]) + '&'
        url += str(historical_url_param_name) + '='
        historical_time_str = time_period_date_list[0] + '/' + time_period_date_list[-1]
        logger.info('[process_event] URL base historico: ' + url)
        logger.info('[process_event] Dias del historico: ' + str(time_period_days))
        logger.info('[process_event] Periodo del historico: ' + historical_time_str)
        logger.info('[process_event] Event procesado exitosamente')
        return url, time_period_date_list, historical_time_str, historical_initial_date.strftime("%Y-%m-%d"), end_point
    except KeyError as e:
        logger.error('[process_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise UserError(error_msg)
    except ValueError as e:
        logger.error('[process_event] El event no tiene los datos esperados. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise UserError(error_msg)
    except Exception as e:
        logger.error('[process_event] Error inesperado al procesar el event. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)
        
        
def create_deltatime(historical_time_period):
    """
    A partir del diccionario entregado por el event crea un deltatime correspondiente al peridodo que se quiere correr una
    fecha
    """
        historical_time_str = ''
        if 'years' in historical_time_period:
            try:
                years_period = int(historical_time_period['years'])
                historical_time_str += ' ' + str(years_period) + ' anios'
            except ValueError:
                years_period = 0
        else:
            years_period = 0
        if 'months' in historical_time_period:
            try:
                months_period = int(historical_time_period['months'])
                historical_time_str += ' ' + str(months_period) + ' meses'
            except ValueError:
                months_period = 0
        else:
            months_period = 0
        if 'weeks' in historical_time_period:
            try:
                weeks_period = int(historical_time_period['weeks'])
                historical_time_str += ' ' + str(weeks_period) + ' semanas'
            except ValueError:
                weeks_period = 0
        else:
            weeks_period = 0
        if 'days' in historical_time_period:
            try:
                days_period = int(historical_time_period['days'])
                
                historical_time_str += ' ' + str(days_period) + ' dias'
            except ValueError:
                days_period = 0
        else:
            days_period = 0
        if historical_time_str == '':
                raise ValueError("event[...]['historical_url_param']['*_time_period'] no incluye a ['years'], ['months'], ['weeks'] o ['days'] valido.")
        logger.debug('[get_historical_data] historical_time_str: ' + historical_time_str)
        time_period_timedelta = rd.relativedelta(days=days_period, weeks=weeks_period, months=months_period, years=years_period)
        return time_period_timedelta


def get_historical_data(incomplete_url, list_of_dates):
    """
    Consulta y retorna datos obtenidos al realisar GET requiest a la API al combinar el imcomplete_url con el list_of_dates
    """
    error_msg = 'Fallo al obtener los datos del historico'
    try: 
        logger.info('[get_historical_data] Iniciando la obtencion de los datos del historico ...')
        data = []
        for historical_date in list_of_dates:
            size = 0
            full_url = incomplete_url + historical_date
            api_response_has_more_data = True
            while api_response_has_more_data:
                api_response = requests.get(full_url)
                api_response_code = api_response.status_code
                if api_response_code == 200:
                    api_response_data = api_response.json()
                    data += api_response_data['data']
                    api_response_has_more_data = api_response_data['meta']['page']['has_more']
                    if not eval(api_response_has_more_data):
                        api_response_has_more_data = False
                    log_msg = 'code: 200, data agregada. url: ' + full_url 
                elif api_response_code == 204:
                    log_msg = 'code: 204, no data. url: ' + full_url 
                    api_response_has_more_data = False
                else:
                    api_response_data = api_response.json()
                    api_response_msg = 'code: ' + str(api_response_code) + ', body: ' + str(api_response_data) + '. url: ' + full_url
                    logger.info('[get_historical_data] Repuesta del API: ' + api_response_msg)
                    logger.error('[get_historical_data] La respuesta del API contiene un codigo de error.')
                    raise Exception('El API no respondio satisfactoriamente a la solicitud.')
                size += max_total_results
                full_url += '&inicio=' + str(size)
                logger.info('[get_historical_data] api_response: ' + log_msg)
        logger.info('[get_historical_data] Obtencion de los datos del historico exitosa')
        return data
    except Exception as e:
        logger.error('[get_historical_data] Error inesperado al obtener los datos del historico. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)

def create_body(historical_data_dict, time_period_str, query_date, end_point):
    """
    Apartir de la informacion consultada crea un body con las llaves esperadas por la maquinas de estados
    """
    logger.info("[create_body] Creando 'body' para la respuesta ...")
    body = {
        "end_point": end_point,
        "data": historical_data_dict,
        "time_period": time_period_str,
        "query_date": query_date
    }
    logger.info("[create_body]'body' creado exitosamente")
    return body

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

