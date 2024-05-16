import hashlib
import os
import boto3
import json
import logging
import utils
from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def download_from_s3_origin(bucket_name, path_s3, name_file):
    """Obtiene la matriz tc del S3"""
    try:
        s3 = boto3.client('s3')
        logger.info(f"Ruta del archivo a descargar: {path_s3+name_file}")
        response = s3.get_object(Bucket=bucket_name, Key=path_s3+name_file)
        data_file = response['Body'].read()
        logger.info("[lambda_function] (download_from_s3_origin) Se obtuvo el archivo de la matriz del Bucket origen correctamente")
        return data_file
    except Exception as e:
        logger.error(f"[lambda_function] (download_from_s3_origin) Ocurrio un error en la descarga de la matriz del bucket {bucket_name}. ERROR: {e}")
        logger.error("[lambda_function] (download_from_s3_origin) Es posible que la matriz no se haya generado") 
    
def generate_sha256_from_s3_file(data_file, file_name):
    """Genera el sha256 del archivo original que viene de s3"""
    try: 
        hash_value = hashlib.sha256(data_file).hexdigest()
        hash_file_name = 'FC_'+file_name.split('/')[-1] + '.sha256'
        logger.info(f"[lambda_function] (generate_sha256_from_s3_file)Se ha generado el sha correctamente del archivo {file_name}")
        return hash_value, hash_file_name
    except (Exception, ):
        logger.error(f"[lambda_function] (generate_sha256_from_s3_file) Ocurrio un error en la generacion del sha del archivo {file_name}")


def move_to_sftp(sftp_client, data, name_file, valuation_date, ftp_path):
    """¨Publica la matriz y el sha en el FTP de clientes"""
    name_file = name_file.split('/')[-1]
    route_path = ftp_path+valuation_date+"/"+name_file
    logger.info(f"[lambda_function] (move_to_sftp) Fecha carpeta a publicar {valuation_date}")
    try:
        with sftp_client.open(route_path, 'wb') as f:
            f.write(data)
        logger.info(f"Se ha publicado el archivo {name_file} correctamente")
    except Exception as e:
        logger.error(f"[lambda_function] (move_to_sftp) Ocurrio un error en la publicacion del archivo: {name_file}")
    

def download_ftp(sftp, ftp_path, name_file, valuation_date):
    """Descarga el archivo y sha de FTP de clientes"""
    name_file = name_file.split('/')[-1]
    route_path = ftp_path+valuation_date+"/"+name_file
    try:
        with sftp.open(route_path, 'rb') as remote_file:
            file_data = remote_file.read()
        logger.info(f"[lambda_function] (download_ftp)Se ha descargado el archivo {name_file} correctamente del FTP")
        return file_data
    except Exception as e:
        logger.error(f"[lambda_function] (download_ftp)Ocurrio un error en la descarga del FTP del archivo: {name_file}")

def upload_s3(bucket_name, path_s3_validator, data, file_name):
    """Se sube los archivos al bucket de validacion"""
    try:
        s3 = boto3.client('s3')
        s3.put_object(Body=data, Bucket=bucket_name, Key=path_s3_validator+file_name)
        logger.info(f"[lambda_function] (upload_s3)Se ha cargado el archivo {file_name} en el bucket {bucket_name} correctamente")
    except Exception as e:
        logger.error(f"[lambda_function] (upload_s3)Ocurrio un error en la carga de la matriz del bucket {bucket_name}. ERROR: {e}")
    
def lambda_handler(event, context):
    bucket_name_origin = get_enviroment_variable('BUCKET_NAME_ORIGIN')
    bucket_name_validator = get_enviroment_variable('BUCKET_NAME_VALIDATOR')
    path_matriz_origin = get_enviroment_variable('PATH_S3_MATRIZ')
    path_s3_validator = get_enviroment_variable('PATH_S3_VALIDATOR')
    name_file_matriz_tc = 'Matriz_TC_YYYYMMDD.txt'
    dates_publish = event["valuation_date"]
    sftp, route_path = utils.connection_sftp()
    for valuation_date in dates_publish:
        date_for_file = valuation_date.replace("-", "")
        name_file_matriz = name_file_matriz_tc.replace("YYYYMMDD", valuation_date.replace("-", ""))
        data_file_origin = download_from_s3_origin(bucket_name_origin, path_matriz_origin, name_file_matriz)
        hash_value, hash_file_name = generate_sha256_from_s3_file(data_file_origin, name_file_matriz)
        move_to_sftp(sftp, data_file_origin, name_file_matriz, valuation_date, route_path)
        move_to_sftp(sftp, hash_value, hash_file_name, valuation_date, route_path)
        data_original = download_ftp(sftp, route_path, name_file_matriz, valuation_date)
        data_hash = download_ftp(sftp, route_path, hash_file_name, valuation_date)
        upload_s3(bucket_name_validator, path_s3_validator, data_original, name_file_matriz)
        upload_s3(bucket_name_validator, path_s3_validator, data_hash, hash_file_name)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
