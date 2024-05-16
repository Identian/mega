#
# =============================================================
#
# Nombre: sftp_collector_utils.py
# Tipo: Modulo
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Reune varios metodos utilizados para enviar arhcivos por SFTP
para Precia en AWS
"""
#
# Variables de entorno:
# Keys:
# - Ninguna
#
# Requerimientos:
# Ninguna libreria de terceros
#
# =============================================================
#

import base64
import boto3
import json
import logging
import os
import sys

import paramiko

logger = logging.getLogger() # Diccionario equivalencias entre columnas del db y params del url
logger.setLevel(logging.INFO)


def get_environment_variable(variable_key): 
    """
    Obtiene la variable de entorno bajo la llave 'variable_key', sino lo encuentra lanza un exception
    :param variable_key: String de la llave de la variable de entorno configurada en el Lambda AWS, normalmente en
    mayusculas
    :return: String con el valor relacionado con la llave 'variable_key'
    """
    try:
        logger.info('[collector_api_utils.get_environment_variables] Obteniendo variables de entorno "' + variable_key + '" ...')
        variable_value = os.environ[str(variable_key)]
        logger.info('[collector_api_utils.get_environment_variables] Obtenida con exito')
        return variable_value
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[collector_api_utils.get_environment_variables] No se encontro variable de entorno. Fallo en linea: ' + error_line + '. faltante: ' + str(e))
        raise Exception ('Variable de entorno no encontrada')


def get_secret(secret_region, secret_name):
    """
    Obtiene credenciales almacenadas en el servicio Secrets Manager de AWS
    :param secret_region: String de la region donde se almaceno el secreto/credenciales
    :param secret_name: String del nombre del secreto en el servicio AWS
    :return: Diccionario con las credenciales almacenadas en el servicio AWS
    """
    try:
        logger.info('[collector_api_utils.get_secret] Intentando obtener secreto: '+ secret_name +'...')
        sesion = boto3.session.Session()
        cliente_secrets_manager = sesion.client(service_name='secretsmanager', region_name=secret_region)
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if 'SecretString' in secret_data:
            secret_str = secret_data['SecretString']
        else:
            secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[collector_api_utils.get_secret] Se obtuvo el secreto exitosamente')
        return json.loads(secret_str)
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[collector_api_utils.get_secret] No se obtuvo el secreto ' + secret_name + ' . Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        raise Exception ('Fallo en obtener el secreto de la base de datos.')
        
        
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
        logger.debug('[collector_api_utils.create_error_response] Creando respuesta: error ...')
        error_response = {'statusCode': status_code}
        body = {'statusCode': status_code,
                'error_type': error_type,
                'error_message': error_message}
        stack_trace = {'log_group': str(context.log_group_name),
                       'log_stream': str(context.log_stream_name),
                       'request_id': str(context.aws_request_id)}
        body['stack_trace'] = stack_trace
        error_response['body'] = json.dumps(body)
        logger.debug('[collector_api_utils.create_error_response] Respuesta creada.')
        return error_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[collector_api_utils.create_error_response] No se pudo crear la respuesta: error. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
            
def conect_to_sftp(secret_sftp_region, secret_sftp_name):
    """
    Se conecta a un SFTP usando las credenciales contenidas en 'sftp_credentials_dict'
    y la libreria 'paramiko' de la capa 'capa-pandas-data-transfer'. Las credenciales
    'sftp_credentials_dict' se almacenan en Secrets Manager AWS, para configurar el
    secreto a usar cambiar las variables de entorno 'SECRET_SFTP_NAME' y
    'SECRET_SFTP_REGION'
    :returns: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
    """
    try:
        sftp_credentials_dict = get_secret(secret_sftp_region, secret_sftp_name)
        logger.info('[sftp_collector_utils.conect_to_sftp] Validando formato de las credenciales del SFTP ...')
        sftp_host = sftp_credentials_dict['host']
        sftp_port = sftp_credentials_dict['port']
        sftp_username = sftp_credentials_dict['username']
        sftp_password = sftp_credentials_dict['password']
        logger.info('[sftp_collector_utils.conect_to_sftp] Credenciales del SFTP tienen el formato esperado.')
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())  # Lambda no almacena host, para ella todos son desconocidos
        logger.info(
            '[sftp_collector_utils.conect_to_sftp] Intentando conectarse a : ' + sftp_host + ', ' + sftp_port + ' ...')
        client.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password, timeout=1)
        sftp = client.open_sftp()
        logger.info('[sftp_collector_utils.conect_to_sftp] Conexion exitosa.')
        return sftp
    except KeyError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
        logger.error(
            '[sftp_collector_utils.conect_to_sftp] Las credenciales no tiene el formato esperado. Fallo en linea: ' + error_line + '. Falta: ' + str(
                e))
        raise Exception('Diccionario de las credenciales del SFTP no tienen todas las llaves esperadas.')
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
        logger.error(
            '[sftp_collector_utils.conect_to_sftp] No se pudo conectarse al SFTP. Fallo en linea: ' + error_line + '. Motivo: ' + str(
                e))
        raise Exception('No se pudo conectar al SFTP')