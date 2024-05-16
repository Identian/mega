"""
=============================================================

Nombre: lbd-dev-etl-src-otc-swp-local-dv01
Tipo: Lambda AWS

Autor:
    - Ruben Antonio Parra Medrano
    - Héctor Augusto Daza Roa
Tecnología - Precia

Ultima modificación: 06/12/2022


Lambda desarrollada para extraer, transformar y cargar los
datos de los archivos dv01 suministrados por los brokers.
Para el caso de Tradiction se multiplica por 2 conforme a
lo informado por el proveedor


=============================================================
"""


from datetime import datetime
from botocore.client import Config
import boto3
import json

import pandas as pd
import requests
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_exceptions import UserError, PlataformError
from precia_utils.precia_response import create_error_response
from precia_utils.precia_aws import (
    get_config_secret,
    get_environment_variable,
    get_secret,
)
from ETLutils import connect_to_db


COLUMN_DICT = {
    "Fecha": "date_dv01",
    "Curva": "instrument_dv01",
    "Tenor": "tenor_dv01",
    "DV01": "dv01",
}
FILE_NAME = "DV01 SIN IDENTIFICAR"
BROKER = "SIN INDENTIFICAR"
EXPECTED_FILE_COLUMNS = ["Fecha", "Curva", "Tenor", "DV01"]
MAX_FILE_SIZE = 5e7
ETL_FORMAT_DATE = "%Y-%m-%d"
FILE_FORMAT_DATE = "%d/%m/%Y"
pd.options.display.float_format = '{:20,.2f}'.format
try:
    logger = setup_logging()
    logger.info("[INIT] Inicializando funcion Lambda ...")
    config_secret_name = get_environment_variable("CONFIG_SECRET_NAME")
    CONFIG_SECRET = get_config_secret(config_secret_name)
    VALUATION_DATE = CONFIG_SECRET["test/valuation_date"]
    try:
        datetime.strptime(VALUATION_DATE, ETL_FORMAT_DATE)
    except ValueError:
        VALUATION_DATE = datetime.now().strftime(ETL_FORMAT_DATE)
    logger.info("Fecha de valoracion: %s", VALUATION_DATE)
    secret_region = CONFIG_SECRET["secret/region"]
    API_URL = CONFIG_SECRET["url/api_precia"]
    BROKERS_LIST = get_environment_variable("BROKERS_NAME").split(",")
    ENABLED_NOTFICATIONS = get_environment_variable("NOTIFICATION_ENABLED")
    s3_config = Config(connect_timeout=1, retries={"max_attempts": 0})
    s3_client = boto3.client("s3", config=s3_config)
    DATA_LAKE = CONFIG_SECRET["s3/bucket"]
    notification_emails = CONFIG_SECRET["validator/mail_to"]
    RECIPIENTS = notification_emails.replace(" ", "").split(",")
    secret_utils_db_name = CONFIG_SECRET["secret/db_precia_utils"]
    secret_scr_db_name = CONFIG_SECRET["secret/db_src_otc"]
    scr_db_secret = get_secret(secret_region, secret_scr_db_name)
    DB_CONNECTION = connect_to_db(scr_db_secret)
    utils_db_secret = get_secret(secret_region, secret_utils_db_name)
    DB_UTILS_CONNECTION = connect_to_db(utils_db_secret)
    DB_STATUS_TABLE = get_environment_variable("DB_STATUS_TABLE")
    DB_TABLE = get_environment_variable("DB_TABLE")
    DV01_SCALES = json.loads(get_environment_variable("DV01_SCALES"))
    print(type(DV01_SCALES))
    logger.info("[INIT] Inicialización finalizada exitosamente.")
except (Exception,):
    logger.error(create_log_msg("Fallo la inicializacion"))
    FAILED_INIT = True
else:
    FAILED_INIT = False

def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en Lambdas AWS
    :param event: Dict con los datos entregados a la Lambda por desencadenador
    :param context: Objeto con el contexto del Lambda suministrado por AWS
    """
    error_msg = "No se pudo completar el lambda_handler"
    try:
        if FAILED_INIT:
            raise PlataformError("Fallo la inicializacion de la funcion lambda")
        logger.info("Inicia la ejecucion de la funcion ...")        
        logger.info("event: %s", json.dumps(event))
        full_s3_file_path, file_size = process_event(event)
        update_status("Ejecutando")
        is_empty_file = validate_file_structure(file_size)
        if is_empty_file:
            update_status("Exitoso")
            body = "Finaliza la ejecucion exitosamente por que el archivo esta vacio"
            send_warning_email(body)
            logger.info(body)
            response = {"statusCode": 200, "body": body}
            return response
            
        full_tmp_file_path = download_file_from_bucket(full_s3_file_path)
        file_data_df = load_file_data(full_tmp_file_path)
        file_data_df = scale_dv01(file_data_df)
        
        if file_data_df.empty:
            update_status("Exitoso")
            body = "Finaliza la ejecucion exitosamente por que no encontraron datos en el archivo"
            send_warning_email(body)
            logger.info(body)
            response = {"statusCode": 200, "body": body}
            return response
        update_database(file_data_df)
        reponse_message = f"Procesamiento del archivo {FILE_NAME} exitoso."
        response = {"statusCode": 200, "body": reponse_message}
        update_status("Exitoso")
        logger.info("Finaliza la ejecucion exitosamente!")
        return response
    except (PlataformError, UserError) as known_exc:
        logger.critical(create_log_msg(error_msg))
        error_response = create_error_response(500, str(known_exc), context)
        report_error(error_response)
        return error_response
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        msg_to_user = "Error desconocido, solicite soporte"
        error_response = create_error_response(500, msg_to_user, context)
        report_error(error_response)
        return error_response

def report_error(error_response: dict):
    """
    Ejecuta todas las funciones que permiten notificar el error al ejecutar el ETL

    Args:
        error_response (dict): Resume el error de la ejecucion con el stack_trace
    """
    try:
        logger.info("Notificando el error ...")
        update_status("Fallido")
    except (Exception,):
        logger.critical(create_log_msg("No se pudo actualizar el status: Fallido"))
    try:
        send_error_email(error_response)
        logger.info("Error notificado")
    except (Exception,):
        log_msg = "No se pudo enviar el correo de notificacion de error"
        logger.critical(create_log_msg(log_msg))


def validate_file_structure(file_size: str) -> bool:
    """
    valida el archivo desde el peso del mismo

    Args:
        file_size (str): Tamanio del archivo insumo

    Raises:
        PlataformError: No supera la validacion de estructura

    Returns:
        bool: True si el archivo esta vacio
    """
    error_msg = "El archivo insumo no supero la validacion de estructura"
    empty_file = False
    try:
        logger.info("Validando la estructura del archivo ... ")
        file_size = int(file_size)
        if file_size > MAX_FILE_SIZE:
            logger.info(
                "El archivo %s es sospechosamente grande, no sera procesado.", FILE_NAME
            )
            raise_msg = f"El archivo {FILE_NAME} supera el tamanio maximo aceptado. "
            raise_msg += "Maximo: {MAX_FILE_SIZE}B. Informado: {file_size}B"
            raise PlataformError(raise_msg)
        if file_size == 0:
            logger.info("El archivo %s esta vacio.", FILE_NAME)
            empty_file = True
        logger.info("Validacion finalizada.")
        return empty_file
    except PlataformError:
        logger.critical(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise


def process_event(event_dict: dict) -> tuple:
    """
    Procesa el event dado por el trigger S3

    Args:
        event_dict (dict): event dado por el trigger S3

    Raises:
        PlataformError: El event del trigger no tiene el formato o los datos esperados

    Returns:
        tuple: ruta del archivo en S3, tamanio del archivo
    """
    error_msg = "El event del trigger no tiene el formato o los datos esperados"
    try:
        logger.info("Procesando y validando event ... ")
        file_path_s3 = event_dict["Records"][0]["s3"]["object"]["key"]
        logger.info("Ruta del archivo en S3: %s", file_path_s3)
        global FILE_NAME
        FILE_NAME = file_path_s3.split("/")[-1]
        logger.info("Nombre del archivo: %s", FILE_NAME)
        global BROKER
        BROKER = FILE_NAME.split("_")[1]
        if BROKER[:3] == "GFI":
            BROKER = "GFI"
        logger.debug("Broker: %s", BROKER)
        s3_bucket = event_dict["Records"][0]["s3"]["bucket"]["name"]
        if s3_bucket != DATA_LAKE:
            raise_msg = "Esta funcion Lambda no fue lanzada desde un bucket conocido,"
            raise_msg += f" se ignora la solicitud. Esperado: {DATA_LAKE}. Informado: {s3_bucket}"
            raise PlataformError(raise_msg)
        logger.info("Bucket S3: %s", s3_bucket)
        file_size_s3 = event_dict["Records"][0]["s3"]["object"]["size"]
        logger.debug("Tamanio del archivo: %s", file_size_s3)
        if "mail" in FILE_NAME or BROKER not in BROKERS_LIST:
            raise_msg = f"El archivo {file_path_s3} no puede procesarse con este ETL."
            raise PlataformError(raise_msg)
        logger.info("Event procesado y validado exitosamente.")
        return file_path_s3, file_size_s3
    except PlataformError:
        logger.critical(create_log_msg(error_msg))
        raise
    except KeyError as key_exc:
        logger.critical(create_log_msg(error_msg))
        raise PlataformError("El event no tiene la estructura esperada") from key_exc
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise


def download_file_from_bucket(full_bucket_path: str) -> str:
    """
    Descarga el archivo informado por event del trigger desde S3 AWS

    Args:
        full_bucket_path (str): Ruta del archivo en el bucket S3

    Raises:
        PlataformError: Cuando no es posible descargar el archivo en S3

    Returns:
        str: Ruta donde se descargo el archivo
    """
    error_msg = "No se pudo descargar el archivo del bucket s3"
    try:
        logger.info("Descargando el archivo %s de %s ... ", full_bucket_path, DATA_LAKE)
        full_lambda_path = "/tmp/" + FILE_NAME
        s3_client.download_file(DATA_LAKE, full_bucket_path, full_lambda_path)
        logger.info("Archivo descargado como %s", full_lambda_path)
        return full_lambda_path
    except (Exception,) as down_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = (
            f"El servicio S3 AWS nego la descarga del archivo {full_bucket_path}"
        )
        raise PlataformError(raise_msg) from down_exc


def load_file_data(lambda_file_path: str) -> pd.DataFrame:
    """
    Carga los datos del archivo insumo en un DataFrame Pandas

    Args:
        lambda_file_path (str): Ruta del archivo insumo en la lambda

    Raises:
        UserError: Cuando el archivo insumo no tiene los datos o formato esperado

    Returns:
        pd.DataFrame: DataFrame con los datos para la base de datos
    """
    error_msg = "No se pudo transformar la informacion"
    try:
        logger.info("Extrayendo los datos del archivo como dataframe Pandas ...")
        file_extension = FILE_NAME.split(".")[-1].lower()
        if file_extension == "csv":
            data_df = pd.read_csv(lambda_file_path)
            if data_df.shape[1] < len(EXPECTED_FILE_COLUMNS):
                data_df = pd.read_csv(lambda_file_path, sep=";")
        elif file_extension == "xlsx":
            data_df = pd.read_excel(lambda_file_path, engine="openpyxl")
        else:
            raise UserError(f"El archivo {FILE_NAME} no puede procesarse con este ETL.")
        logger.info("Extraccion de los datos exitosa.")
        logger.info(
            "Datos orginales del archivo. data_df(5):\n" + data_df.head().to_string()
        )
        data_df.dropna(axis = 0, how='all', inplace=True)
        data_df.dropna(axis=1, how='all', inplace=True)
        data_df = data_df.loc[:, ~data_df.columns.str.contains("^Unnamed")]
        data_columns = list(data_df.columns)
        if set(data_columns) != set(EXPECTED_FILE_COLUMNS):
            raise_msg = "El archivo no tiene las columnas esperadas."
            raise_msg += (
                f" Esperadas:{EXPECTED_FILE_COLUMNS}. Encontradas: {data_columns}"
            )
            raise UserError(raise_msg)
        if data_df.empty:
            logger.warning('El DataFrame de los datos del archivo esta vacio.')
            return pd.DataFrame()
        if not (data_df["Fecha"] == data_df['Fecha'].iloc[0]).all():
            raise_msg = "La columna 'Fecha' No tiene el mismo valor en todas las filas."
            raise UserError(raise_msg)
        logger.info(
            "Transformando los datos del archivo para actualizar la base de datos ..."
        )
        data_df.rename(columns=COLUMN_DICT, inplace=True)
        logger.debug("Nuevas columnas. data_df(5):\n%s", data_df.head().to_string())
        data_df["systems"] = BROKER.upper()
        data_df["instrument_dv01"] = data_df["instrument_dv01"].replace(
            ["IBRSOFR"], "USDCO", regex=True
        )
        data_df["date_dv01"] = pd.to_datetime(data_df["date_dv01"], format=FILE_FORMAT_DATE)
        data_df["date_dv01"] = data_df["date_dv01"].dt.strftime(ETL_FORMAT_DATE)
        data_valuation_date = data_df['date_dv01'].iloc[0]
        global VALUATION_DATE
        if data_valuation_date != VALUATION_DATE:
            log_msg = (
                "La fecha de valoracion del archivo no coincide con la configurada. "
            )
            log_msg += (
                "Si esta en pruebas, ignore esta advertencia, en otro caso solicite "
            )
            log_msg += f"soporte. Configurada: {VALUATION_DATE}. Encontrada: {data_valuation_date}"
            logger.warning(log_msg)
            send_warning_email(log_msg)
            VALUATION_DATE = data_valuation_date
        logger.info(
            "Datos limpiados del archivo. data_df(5):\n" + data_df.head().to_string()
        )
        data_df["dv01"] = pd.to_numeric(data_df["dv01"])#, downcast='float')
        if BROKER == "GFI":
            data_df["instrument_dv01"] = data_df["instrument_dv01"].replace(
                ["IRS COP IBR"], "IBR", regex=True
            )
        logger.info("Escalar. data_df(5):\n" + data_df.head().to_string())
        data_df["tenor_dv01"] = data_df["tenor_dv01"].str.upper()
        logger.info("Transformacion de los datos exitosa.")
        return data_df
    except UserError:
        logger.critical(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise

def scale_dv01(unscaled_df: pd.DataFrame):
    """
    Aplica un escalamiento a todos los valores de DV01 del dataframe,
    según las escalas indicadas en el diccionario 'dv01_scales'
    Args:
        unscaled_df (pd.DataFrame): dataframe con los valores de DV01
            originales
    Returns:
        pd.DataFrame: dataframe con todos los escalamientos aplicados
    """
    error_msg = 'Fallo el escalamiento de los valores de DV01'
    try:
        scaled_df = unscaled_df
        logger.info('BROKER: %s',BROKER)
        logger.debug('unscaled dataframe:\n%s',unscaled_df)
        broker_dv01_scales = DV01_SCALES[BROKER]
        logger.info('broker_dv01_scales:\n%s',broker_dv01_scales)
        logger.info('Unscaled DV01: \n'+ unscaled_df.to_string())
        for instrument in broker_dv01_scales:
            logger.debug("scale: %s",broker_dv01_scales[instrument])
            scaled_df.loc[unscaled_df["instrument_dv01"] == instrument, "dv01"] = (
                unscaled_df["dv01"] * broker_dv01_scales[instrument]
            )
        logger.info('Scaled DV01: \n'+ scaled_df.to_string())
        logger.debug('scaled dataframe:\n%s',scaled_df)
        return scaled_df
    except (Exception,) as scl_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from scl_exc
    

def update_database(data_df: pd.DataFrame):
    """
    Actualiza la base de datos con los datos limpios del archivo insumo

    Args:
        data_df (pd.DataFrame): datos limpios del archivo insumo

    Raises:
        PlataformError: Cuando la base de datos no aceptan los datos del archivo
    """
    error_msg = "No se pudo realizar la insercion de los datos a la base de datos"
    try:
        logger.info("Actualizando la base de datos ...")
        delete_query = f"DELETE FROM {DB_TABLE} WHERE date_dv01 = "
        delete_query += f"'{VALUATION_DATE}' AND systems = '{BROKER}'"
        logger.debug("delete_query: %s", delete_query)
        DB_CONNECTION.execute(delete_query)
        logger.info(
            "Datos para la base de datos. data_df(5):\n%s", data_df.head().to_string()
        )
        logger.info("Insertando datos nuevos en la base de datos ...")
        data_df.to_sql(
            name=DB_TABLE,
            con=DB_CONNECTION,
            if_exists="append",
            index=False
        )
        logger.info("Actualizacion de la base de datos exitosa.")
    except sa.exc.SQLAlchemyError as sql_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = "La base de datos no acepto los datos. validar tipos de datos"
        raise PlataformError(raise_msg) from sql_exc
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise


def update_status(status):
    """
    Actualiza la tabla DB_STATUS_TABLE para informar la maquina de estados del proceso
    sobre status actual de la ETL y por ende del insumo en la base de datos SRC.
    Los status validos son: Ejecutando' cuando se lanza el ETL, 'Exitoso' para cuando
    concluya exitosamente, y 'Fallido' para cuando el archivo no sea procesable.
    :param String que define el status actual de la ETL
    """
    error_msg = "No fue posible actualizar el estado de la ETL en DB"
    try:
        schedule_name = FILE_NAME[:-13]
        status_register = {
            "id_byproduct": "swp_local",
            "name_schedule": schedule_name,
            "type_schedule": "ETL",
            "state_schedule": status,
            "details_schedule": VALUATION_DATE,
        }
        logger.info("Actualizando el status de la ETL a: %s", status)
        status_df = pd.DataFrame([status_register])
        status_df.to_sql(
            name=DB_STATUS_TABLE,
            con=DB_UTILS_CONNECTION,
            if_exists="append",
            index=False,
        )
        logger.info("Status actualizado en base de datos.")
    except (Exception,) as status_exc:
        logger.critical(create_log_msg(error_msg))
        raise_msg = "El ETL no puede actualizar su estatus, el proceso no sera lanzado"
        raise PlataformError(raise_msg) from status_exc


def send_error_email(error_response: dict):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    que el ETL no finalice su ejecucion correctamente.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de error"
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info('Notificaciones desabilitadas en las variables de entorno')
            logger.info('No se enviara ningun correo electronico')
            return None
        logger.info("Creando email de error ... ")
        url_notification = f"{API_URL}/utils/notifications/mail"
        logger.info("URL API de notificacion: %s", url_notification)
        error_msg = json.dumps(error_response, skipkeys=True, allow_nan=True, indent=6)
        subject = f"Optimus-K: Fallo el procesamiento del archivo {FILE_NAME}"
        body = f"""

        Cordial Saludo.

        Durante el procesamiento del archivo insumo {FILE_NAME} del broker {BROKER} \
        se presento un error que detiene el proceso.

        Para conocer mas detalles de lo ocurrido, por favor revisar el log referenciado \
        en este mensaje de error:

        {error_msg}

        Con los valores 'log_group', 'log_stream' y 'request_id' puede ubicar el log en \
        el servicio ColudWatch AWS.

        Optimus K.
        """
        body = body.replace("        ", "")
        payload = {"subject": subject, "recipients": RECIPIENTS, "body": body}
        logger.info("Email creado")
        logger.info("Enviando email de error ... ")
        mail_response = requests.post(url_notification, json=payload, timeout=2)
        api_mail_code = str(mail_response.status_code)
        api_mail_text = mail_response.text
        logger.info(
            "Respuesta del servicio de notificaciones: code: %s, body: %s",
            api_mail_code,
            api_mail_text,
        )
        if mail_response.status_code != 200:
            raise_msg = (
                "El API de notificaciones por email no respondio satisfactoriamente."
            )
            raise PlataformError(raise_msg)
        logger.info("Email enviado.")
        return None
    except PlataformError:
        logger.critical(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise


def send_warning_email(warm_msg: str):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    de una advertencia.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de advertencia"
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info('Notificaciones desabilitadas en las variables de entorno')
            logger.info('No se enviara ningun correo electronico')
            return None
        logger.info("Creando email de advertencia ... ")
        url_notification = f"{API_URL}/utils/notifications/mail"
        logger.info("URL API de notificacion: %s", url_notification)
        subject = (
            f"Optimus-K: Advertencia durante el procesamiento del archivo {FILE_NAME}"
        )
        body = f"""

        Cordial Saludo.

        Durante el procesamiento del archivo insumo {FILE_NAME} del broker {BROKER} \
        se presento una advertencia que NO detiene el proceso.

        Advertencia informada:

        {warm_msg}


        Optimus K.
        """
        body = body.replace("        ", "")
        payload = {"subject": subject, "recipients": RECIPIENTS, "body": body}
        logger.info("Email creado")
        logger.info("Enviando email de error ... ")
        mail_response = requests.post(url_notification, json=payload, timeout=2)
        api_mail_code = str(mail_response.status_code)
        api_mail_text = mail_response.text
        logger.info(
            "Respuesta del servicio de notificaciones: code: %s, body: %s",
            api_mail_code,
            api_mail_text,
        )
        if mail_response.status_code != 200:
            raise_msg = (
                "El API de notificaciones por email no respondio satisfactoriamente."
            )
            raise PlataformError(raise_msg)
        logger.info("Email enviado.")
        return None
    except PlataformError:
        logger.critical(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        raise