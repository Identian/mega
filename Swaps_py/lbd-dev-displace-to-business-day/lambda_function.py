#
# =============================================================
#
# Nombre: lbd-dev-displace-to-business-day.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
A partir de la informacion validada del event se realiza el
corrimiento de una fecha calendario a una fecha habil
conforme al intervalo informado, si la fecha calendario
no corresponde a un dia habil se lanza una exception, sino
se retorna la fecha solicitada.
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
import json
import logging
import sys

def lambda_handler(event, context):
    """
    Equivalente lambda de la funcion main, recibe la informacion del desencadenante event
    """
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        # Validando Event
        calendar_list = event['calendar']['data']
        valuation_date = event['valoration_date']
        if valuation_date == '<<<TODAY>>>': 
            today_date = datetime.now() - rd.relativedelta(hours=5)
            valuation_date = datetime.strftime(today_date, '%Y-%m-%d')
        days_shift = int(event['shift']['days'])
        if not valuation_date in calendar_list:
            raise Exception('Hoy no es dia bursatil')
        business_date_index = calendar_list.index(valuation_date) + days_shift
        business_date = calendar_list[business_date_index]
        new_event = event
        new_event['displaced_business_date'] = business_date
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente.")
        return new_event
    except Exception as  e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(create_error_response(500, str(e), context))


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