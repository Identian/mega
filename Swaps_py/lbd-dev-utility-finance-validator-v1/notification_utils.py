#
# =============================================================
#
# Nombre: notification_utils.py
# Tipo: Modulo
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Reune varios metodos utilizados para crear notificaciones para
Precia en AWS
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

import logging
import os
import boto3
import base64
import json 
import smtplib
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_environment_variable(variable_key):
    """
    Obtiene la variable de entorno bajo la llave 'variable_key', sino lo encuentra lanza un exception
    :param variable_key: String de la llave de la variable de entorno configurada en el Lambda AWS, normalmente en
    mayusculas
    :return: String con el valor relacionado con la llave 'variable_key'
    """
    try:
        logger.info('[notification_utils.get_secret] Obteniendo variables de entorno "' + variable_key + '" ...')
        variable_value = os.environ[str(variable_key)]
        logger.info('[notification_utils.get_secret] Obtenida con exito')
        return variable_value
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[notification_utils.get_secret] (get_enviroment_variable) No se encontró las variables de entorno' + str(e))
        raise Exception('Variable de entorno no encontrada')


def get_secret(secret_region, secret_name):
    """
    Obtiene credenciales almacenadas en el servicio Secrets Manager de AWS
    :param secret_region: String de la region donde se almaceno el secreto/credenciales
    :param secret_name: String del nombre del secreto en el servicio AWS
    :return: Diccionario con las credenciales almacenadas en el servicio AWS
    """
    try:
        logger.info('[notification_utils.get_secret] Intentando obtener secreto: ' + secret_name + '...')
        session = boto3.session.Session()
        client_secrets_manager = session.client(service_name='secretsmanager', region_name=secret_region)
        secret_data = client_secrets_manager.get_secret_value(SecretId=secret_name)
        if 'SecretString' in secret_data:
            secret_str = secret_data['SecretString']
        else:
            secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[notification_utils.get_secret] Se obtuvo el secreto.')
        return json.loads(secret_str)
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[notification_utils.get_secret] No se obtuvo el secreto ' + secret_name + ' . Fallo en linea: ' + error_line + '. Motivo: ' + str(
                e))
        raise Exception('Fallo en obtener el secreto del SMTP.')
    

def create_error_response(status_code, error_type, error_message, context):
    """
    Crea la respuesta del Lambda en caso de error, cumple el HTTP protocol version 1.1 Server
    Response Codes. Entre los valores que el diccionario que retorna se encuentra 'log_group',
    'error_message' y 'request_id' que permite buscar el log en CloudWatch AWS
    :param status_code: Integer de codigo de respuesta del servidor 4XX o 5XX conforme al
    HTTP protocol version 1.1
    Server Response Codes
    :param error_type: String del tipo de error relacionado con 'status_code' conforme al
    HTTP protocol version 1.1
    Server Response Codes
    :param error_message: String con un mensaje en espaniol para que el usuario del API
    :param context: Contexto del Lambda AWS
    :return: Diccionario con la respuesta lista para retornar al servicio API Gateway AWS
    """
    try:
        logger.debug('[notification_utils.create_error_response] Creando respuesta: error ...')
        error_response = {'statusCode': status_code}
        body = {'statusCode': status_code,
                'error_type': error_type,
                'error_message': error_message}
        stack_trace = {'log_group': str(context.log_group_name),
                       'log_stream': str(context.log_stream_name),
                       'request_id': str(context.aws_request_id)}
        body['stack_trace'] = stack_trace
        error_response['body'] = json.dumps(body)
        logger.debug('[notification_utils.create_error_response] Respuesta creada.')
        return error_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[notification_utils.create_error_response] No se pudo crear la respuesta: error. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
            

def connect_to_smtp():
    """
    Se conecta al servio SMTP de Precia
    """
    secret_smtp_name = get_environment_variable('SMTP_SECRET_NAME')
    secret_smtp_region = get_environment_variable('SMTP_SECRET_REGION')
    smtp_secret = get_secret(secret_smtp_region, secret_smtp_name)
    try:
        logger.info('[notification_utils.connect_to_smtp] Intentando Conectarse al SMTP ...')
        connection = smtplib.SMTP(host=smtp_secret['server'], port=smtp_secret['port'])
        connection.starttls()
        connection.login(smtp_secret['user'], smtp_secret['password'])
        return connection
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[notification_utils.connect_to_smtp] No se pudo conectar al SMTP. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        raise Exception('Fallo la conexion al SMTP.')