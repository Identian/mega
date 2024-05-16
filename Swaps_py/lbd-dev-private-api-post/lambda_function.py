#
# =============================================================
#
# Nombre:
# lbd-dev-private-api-post.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Por medio de la libreria request realiza solicitudes POST
a la API privada de Precia en AWS
"""
#
# Variables de entorno:
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


def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en Lambas AWS
    :param event: JSON con los datos entregados a la Lambda por el desencadenador
    :param context: JSON con el contexto del Lambda suministrado por AWS
    :return: JSON con los datos solicitados o mensaje de error
    """
    logger.debug('[lambda_handler] event: ' + str(json.dumps(event)))
    
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        data_url, data_dict = process_event(event)
        api_response = post_data(data_url, data_dict)
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente .")
        return {
            'statusCode': 200,
            'body': api_response
        }
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
    error_msg = 'No se pudo obtener el URL y el payload a partir del event'
    try: 
        logger.info("[process_event] Procesando event para obtener URL y payload ...")
        api_endpoint = trigger_event['url']
        data_list = trigger_event['data']
        if not isinstance(data_list, list):
            raise UserError('Los datos bajo la llave "data" debe ser una lista de diccionarios.')
        logger.info("[process_event] URL y payload obtenido exitosamente.")
        return api_endpoint, {"data": data_list}
    except KeyError as e:
        logger.error('[process_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise UserError('No se encontro ' + str(e))
    except UserError as e:
        logger.error('[process_event] El event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise UserError(str(e))
    except Exception as  e:
        logger.error('[process_event] Error inesperado al procesar el event. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)
            

def post_data(url, data):
    """
    Actualiza los datos 'data' por medio de la API privada de Precia al realizar una solicitud POST al endpoint 'url'
    """
    error_msg = 'No se pudo comunicar con la API satisfactoriamente'
    try: 
        logger.info("[post_data] Ejecutando solicitud POST a la API...")
        api_response = requests.post(url, json=data)
        api_response_code = api_response.status_code 
        api_response_data = api_response.text
        api_response_msg = 'code: ' + str(api_response_code) + ', body: ' + str(api_response_data) + '. url: ' + url
        logger.info('[post_data] Repuesta del API: ' + api_response_msg)
        if not api_response_code == 200:
            raise Exception('El API no respondio satisfactoriamente a la solicitud.')
        logger.info("[post_data] Solicitud POST respondida por la API satisfactoriamente.")
        return data
    except Exception as  e:
        logger.error('[post_data] No se completo la solicitud POST. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
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
        

