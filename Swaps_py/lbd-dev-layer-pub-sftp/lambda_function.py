#
# =============================================================
#
# Nombre: lbd-dev-layer-pub-sftp.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
Pone archivos planos ubicados en el bucket S3 AWS de las
variables de entorno en los diferentes SFTPs de en AWS.
"""
#
# Variables de entorno:
# BUCKET_DATALAKE: s3-dev-datalake-sources
# SECRET_SFTP_REGION: us-east-1
#
# Requerimientos:
# capa-pandas-data-transfer
#
# =============================================================
#

import boto3
import json
import logging
import sys

import layer_utils as utils


sftp_secret_dict = {
    'pruebas-ftp.precia.co': 'precia/lambda/collectors/ftp_precia',
    'pruebas_sftp_optimus_b': 'precia/lambda/optimus_k/sftp_optimus_b'
}

logger = logging.getLogger()  # Log, ver en CloaudWatch por log_group, log_stream, request_id
logger.setLevel(logging.DEBUG)

reponse = '' # respuesta para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info("[INIT] Inicializando funcion ...")
    s3_client = boto3.client('s3')
    bucket_datalake = utils.get_environment_variable('BUCKET_DATALAKE')
    sftp_secret_region = utils.get_environment_variable('SECRET_SFTP_REGION')
    logger.info("[INIT] Inicializacion finalizada")
except Exception as e:
    error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
    logger.error('[INIT] No se completo la inicializacion. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
    response = str(e)
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
        global response 
        raise Exception(response)

    logger.info('[lambda_handler] event: ' + str(json.dumps(event)))
   
    try:
        sftp_connection, sftp_file_path, bucket_file_path = transform_event(event['body'])
        logger.info('[lambda_handler] transform_event = OK')
        lambda_file_path = download_file_from_bucket(bucket_datalake, bucket_file_path)
        logger.info('[lambda_handler] download_file_from_bucket = OK')
        load_file_to_sftp(lambda_file_path, sftp_connection, sftp_file_path)
        logger.info('[lambda_handler] load_file_to_sftp = OK')
    except Exception as e:
        raise Exception(str(e))
    return {
        'statusCode': 200,
        'body': json.dumps('Archivo actualizado en sftp')
    }


def transform_event(event_json):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """
    full_s3_path = event_json['file']
    logger.info('[transform_event] full_s3_path = ' + full_s3_path)
    full_s3_path_split = full_s3_path.split('/')
    sftp_name = full_s3_path_split[1]
    logger.info('[transform_event] sftp_name = ' + sftp_name)
    full_sftp_path = '/' + '/'.join(full_s3_path_split[2:])
    logger.info('[transform_event] full_sftp_path = ' + full_sftp_path)
    sftp_secret_name = sftp_secret_dict[sftp_name]
    logger.info('[transform_event] sftp_secret_name = ' + sftp_secret_name)
    sftp_conection = utils.conect_to_sftp(sftp_secret_region, sftp_secret_name)
    return sftp_conection, full_sftp_path, full_s3_path


def download_file_from_bucket(bucket_name, full_bucket_path):
    """
    Descarga archivo ubicado en 'full_bucket_path' en el 'bucket_name' S3 AWS
    """
    filename = full_bucket_path.split('/')[-1]
    logger.info('[download_file_from_bucket] filename = ' + filename)
    full_lambda_path = '/tmp/' + filename
    s3_client.download_file(bucket_name, full_bucket_path, full_lambda_path)
    return full_lambda_path


def load_file_to_sftp(full_lambda_path, connection, full_sftp_path):
    """
    Mueve el archivo 'full_lambda_path' a la ubicacion 'full_sftp_path' en el SFTP indicado
    por el event
    """
    folder = full_sftp_path.replace(full_sftp_path.split('/')[-1], '')
    logger.info('[load_file_to_sftp] folder = ' + folder)
    try: 
        connection.put(full_lambda_path, full_sftp_path)
    except Exception as e:
        connection.mkdir(folder)
        logger.info('[load_file_to_sftp] folder: ' + folder)
        connection.put(full_lambda_path, full_sftp_path)
    