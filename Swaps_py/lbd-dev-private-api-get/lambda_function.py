#
# =============================================================
#
# Nombre:
# lbd-dev-private-api-get.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Por medio de la libreria request realiza solicitudes GET
a la API privada de Precia en AWS
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

from datetime import datetime
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
        data_url = process_event(event)
        data_list = get_data(data_url)
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente .")
        return {"data": data_list}
    except UserError as e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(create_error_response(400, str(e), context))
    except Exception as  e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(create_error_response(500, 'Error al ejecutar la solicitud', context))
        
        
def process_event(trigger_event):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """
    error_msg = 'No se pudo crear el URL a partir del event'
    try: 
        logger.info("[process_event] Procesando event para crear URL ...")
        api_endpoint = trigger_event['endpoint']
        if api_endpoint[-1] == '/' or api_endpoint[-1] == '\\':
            api_endpoint = api_endpoint[:-1] + '?'
        else:
            api_endpoint += '?'
        url_params  = trigger_event['url_params']
        if not isinstance(url_params, dict):
            raise UserError('La llave "url_params" debe tener un diccionario de formato {"nombre": "valor"}')
        today_date = datetime.now() - rd.relativedelta(hours=5)
        url_params_list = list(url_params)
        for param_name in url_params_list:
            param_value = url_params[param_name]
            if param_value == '<<<TODAY>>>':
                param_value = datetime.strftime(today_date, '%Y-%m-%d')
            if isinstance(param_value, dict):
                delta_dict = {
                        'years': 0,
                        'months': 0,
                        'days': 0,
                        'weeks': 0
                    }
                param_type = param_value['type']
                if param_type != 'date':
                    raise UserError('Actualmente solo disponible tipo "date"')
                value = param_value['value']
                if value == '<<<TODAY>>>':
                    value_date = today_date
                else:  
                    try: 
                        value_date = datetime.strptime(value, '%Y-%m-%d')
                    except Exception as e:
                        raise UserError('Las fechas deben tener formato: AAAA-MM-DD')
                delta = int(param_value['delta'])
                unit = param_value['unit']
                if not unit in delta_dict:
                    raise UserError('Las "unit"s actualmente admitidas son ' + str(list(delta_dict)))
                delta_dict[unit] = delta
                time_delta = rd.relativedelta(years=delta_dict['years'], months=delta_dict['months'], days=delta_dict['days'], weeks=delta_dict['weeks'])
                param_date = value_date + time_delta
                param_value = datetime.strftime(param_date, '%Y-%m-%d')
            api_endpoint += str(param_name) + '=' + str(param_value)
            if not param_name == url_params_list[-1]:
                api_endpoint += '&'
        logger.info("[process_event] URL creada  partir del event exitosamente.")
        return api_endpoint
    except KeyError as e:
        logger.error('[process_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise UserError('No se encontro ' + str(e))
    except UserError as e:
        logger.error('[process_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise UserError(str(e))
    except Exception as  e:
        logger.error('[process_event] Error inesperado al procesar el event. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)
            

def get_data(url):
    """
    Obtiene la datos de la API privada de Precia al realizar una solicitud GET al endpoint 'url'
    """
    error_msg = 'No se pudo comunicar con la API satisfactoriamente'
    try: 
        logger.info("[get_data] Ejecutando solicitud GET a la API...")
        data = []
        size = 0
        full_url = url
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
                logger.info('[get_data] Repuesta del API: ' + api_response_msg)
                logger.error('[get_data] La respuesta del API contiene un codigo de error.')
                raise Exception('El API no respondio satisfactoriamente a la solicitud.')
            size += max_total_results
            full_url = url + '&inicio=' + str(size)
            logger.info('[get_data] api_response: ' + log_msg)
        logger.info("[get_data] Solicitud GET respondida por la API satisfactoriamente.")
        return data
    except Exception as  e:
        logger.error('[get_data] No se completo la solicitud GET. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
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
        

