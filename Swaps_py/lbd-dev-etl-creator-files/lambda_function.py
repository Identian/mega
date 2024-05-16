#
# =============================================================
#
# Nombre: lbd-dev-etl-creator-files.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
A partir de la informacion validada del event crea un archivo
plano en bucket S3 AWS de las variables de entorno. Actualmente
solo disponible .txt y .csv.
"""
#
# Variables de entorno:
# BUCKET_DATALAKE: s3-dev-datalake-sources
#
# Requerimientos:
# capa-pandas-requests
#
# =============================================================
#

import boto3
import json
import logging
import sys

import pandas as pd

import etl_utils as utils

logger = logging.getLogger()  # Log, ver en CloaudWatch por log_group, log_stream, request_id
logger.setLevel(logging.INFO)

reponse = '' # respuesta para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info("[INIT] Inicializando funcion ...")
    client_s3 = boto3.client('s3')
    bucket_datalake = utils.get_environment_variable('BUCKET_DATALAKE')
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
        return {
            'statusCode': 500,
            'body': response
        }

    logger.info('[lambda_handler] event: ' + str(json.dumps(event)))
    
    try:
        tmp_file_path = create_file(event['body'])
        full_bucket_path = put_file_to_bucket(tmp_file_path, event['body']['destinations'], bucket_datalake)
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
    return {
        'statusCode': 200,
        'body': {"file": full_bucket_path}
    }
    

def create_file(event_dict):
    """
    Crea el archivo plano segun su extencion
    """
    try:
        file_extension = event_dict['filename']['extension'].replace('.', '')
        if file_extension == 'csv' or file_extension == 'txt':
            file_path = create_csv_txt_file(event_dict)
        else:
            raise Exception('Extencion del archivo a crear no disponible.')
        return file_path
    except Exception as e:
        print(str(e))
        

def create_csv_txt_file(event_dict):
    """
    Crea un archivo plano de extencion txt o csv usando la libreria pandas
    """
    try:
        full_filename = '/tmp/' + event_dict['filename']['name'] + '.' + event_dict['filename']['extension']
        logger.info('[create_csv_txt_file] full_filename = ' + full_filename)
        data_df = pd.DataFrame(event_dict['data']['data_list'])
        logger.info('[create_csv_txt_file] data_df = ' + data_df.to_string())
        if 'precia_id' in event_dict and 'precia_id' in data_df:
            if event_dict['precia_id']['include'] == False:
                data_df = data_df.drop(columns=['precia_id'])                               
        header_config = event_dict['header']
        include_header = header_config['include']
        if 'file_columns' in header_config:
            if isinstance(header_config['file_columns'], list):
                if header_config['file_columns'] != []:
                    data_df = data_df[header_config['file_columns']]
            else:
                raise Exception('Event no valido. header:file_columns debe ser una lista.')
        if 'column_replace_name' in header_config:
            if isinstance(header_config['column_replace_name'], dict):
                data_df.rename(columns = header_config['column_replace_name'], inplace = True)
            else:
                raise Exception('Event no valido. header:column_replace_name debe ser un diccionario.')
        data_config = event_dict['data']
        if 'column_separator' in data_config:
            column_separator = str(data_config['column_separator'])
        else:
            column_separator = ','
        if 'decimal_point' in data_config:
            decimal_point = str(data_config['decimal_point'])
        else:
            decimal_point = '.'
        if decimal_point == column_separator:
            raise Exception('Separador de columnas y el punto decimal son el mismo, puede crear conflictos al momento de la lectura.')
        if 'decimal_format' in data_config:
            decimal_format = str(data_config['decimal_format'])
        else:
            decimal_format = None
        if 'date_format' in data_config:
            date_format = str(data_config['date_format'])
        else:
            date_format = None
        logger.info('[create_csv_txt_file] data_df = ' + data_df.to_string())
        data_df.to_csv(full_filename, index=False, header=include_header, sep=column_separator, decimal=decimal_point, float_format=decimal_format, date_format=date_format)
        logger.info('[create_csv_txt_file] file_df = ' + pd.read_csv(full_filename).to_string())
        return full_filename
    except Exception as e:
        print(str(e))
        

def put_file_to_bucket(full_lambda_path, destinations_list, bucket):
    """
    Pone un archivo plano en un bucket S3 AWS
    """
    filename = full_lambda_path.split('/')[-1]
    try:
        for sftp in destinations_list:
            logger.info('[put_file_to_bucket] sftp = ' + str(sftp))
            full_bucket_path = 'files_to_sftp/' + sftp['sftp_name'] + sftp['sftp_path'] + filename
            logger.info('[put_file_to_bucket] Cargando archivos al bucket: ' + bucket + ' ...')
            client_s3.delete_object(Bucket=bucket, Key=full_bucket_path)
            logger.info('[put_file_to_bucket] Cargando archivo desde: ' + full_lambda_path + ' ...')
            client_s3.upload_file(full_lambda_path, bucket, full_bucket_path)
            logger.info('[put_file_to_bucket] Archivo cargado en: ' + full_bucket_path)
            return full_bucket_path # TODO: DELETE
        logger.info('[put_file_to_bucket] Carga finalizada.')
    except KeyError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
        logger.error(
            "[put_file_to_bucket] la lista 'all_path_list' no tiene el formato esperado. Fallo en linea: " + error_line + '. Falta: ' + str(
                e))
        raise ValueError("No se genero de manera de correcta la lista 'all_path_list'")
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)  # Obtiene el numero de la linea que genero el exception
        logger.error(
            '[put_file_to_bucket] No se pudo cargar los archivos al bucket. Fallo en linea: ' + error_line + '. Motivo: ' + str(
                e))
        raise Exception('Fallo la carga de archivos al bucket')