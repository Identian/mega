#
# =============================================================
#
# Nombre: lbd-dev-etl-otc-swp-local-sample.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Personaliza la creacion de archivos planos de salida
para la muestra local swap
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

import json
import logging
import sys

import pandas as pd


logger = logging.getLogger()  # Log, ver en CloaudWatch
logger.setLevel(logging.INFO)
error_msg = '' # respuesta para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info("[INIT] Inicializando funcion ...")
    sftp_name = utils.get_environment_variable('SFTP_NAME')
    sftp_path = utils.get_environment_variable('SFTP_PATH')
    logger.info("[INIT] Inicializacion finalizada")
except Exception as e:
    error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
    logger.error('[INIT] No se completo la inicializacion. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
    error_msg = str(e)
    failed_init = True

def lambda_handler(event, context):
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        output_file_name = event['output_file_name']
        if output_file_name == 'Swap_IBR_ICAP_1':
            destination_folder = 'Intradia/'
        else:
            destination_folder = ''
        data_df = pd.DataFrame(event['data'])
        data_df.sort_values(by=['dias'], inplace=True)
        data_to_file_list = data_df.to_dict('records')
        # File creator
        valoration_date_str = event['data'][0]['fecha-valoracion'].replace('-', '')
        response = {
            "filename": {
                "name": output_file_name + '_' +valoration_date_str,
                "extension": "csv"
            },
            "header": {
                "include": False,
                "file_columns": ['dias', 'mid', 'bid', 'ask', 'tipo-precio']
            },
            "precia_id": {
                "include": False
            },
            "data": {
                "column_separator": ",",
                "decimal_point": ".",
                "date_format": None,
                "data_list": data_to_file_list
            },
            "destinations": [
                {
                    "sftp_name": sftp_name,
                    "sftp_path": sftp_path + destination_folder
                }
             ]
        }
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente ...")
        return {
            'statusCode': 200,
            'body': response
        }
    except Exception as e:
        logger.critical(
            '[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(
                e))
        raise Exception(create_error_response(500, str(e), context))


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
