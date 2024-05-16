"""
Módulo ETL ICAP
Contiene el código de la lambda lbd-p-etl-src-icap-fwd-swp-local, funcionando en glue.
Este componente procesa el archivo de Tullet que contiene las puntas
"""
from base64 import b64decode
from datetime import datetime
from dateutil import tz, relativedelta as rd
import datetime as dt
import io
import json
import logging
import os
import sys
from sys import stdout, argv, exc_info

from awsglue.utils import getResolvedOptions
from botocore.client import Config
import boto3

import pandas as pd
import paramiko
import requests
import sqlalchemy as sa

pd.options.mode.chained_assignment = None
failed_init = False
error_message = ""


ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)

FILE_NAME = "TPOrderreportfile"
BROKER = "TP"
EXPECTED_FILE_COLUMNS = [
    "Vendor",
    "Issue Description",
    "Trade Date",
    "Order Type",
    "Action Type",
    "Market Entry Time",
    "Market Leave Time",
    "Price",
    "DV01",
    "Currency",
    "Rejected",
]
MAX_FILE_SIZE = 5e7
ETL_FORMAT_DATE = "%Y-%m-%d"
FILE_FORMAT_DATE = "%d/%m/%Y"
SWP_COLUMN_DICT = {
    "Vendor": "sistema",
    "derived_type": "tipo-derivado",
    "Market": "mercado",
    "Underlying": "subyacente",
    "Trade_Date": "fecha",
    "Market_Entry_Time": "hora-inicial",
    "Market_Leave_Time": "hora-final",
    "date_issue": "fecha-emision",
    "Tenor": "tenor",
    "Amount": "nominal",
    "order": "tipo-orden",
    "Price": "precio",
}

#-----------------------------------------------------------------------------------------------
# CONFIGURACIÓN DE LOGS
def setup_logging(log_level):
    """
    Configura el sistema de registro de logs.
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        file_handler = logging.StreamHandler(stdout)
    file_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato de la variable ERROR_MSG_LOG_FORMAT al mensaje log_msg.
    Valida antes de crear el mensaje si existe una excepcion, y de ser el caso
    asocia el mensaje log_msg a los atributos de la excepcion.

    Args:
        log_msg (str): Mensaje de error personalizado que se integrara al log

    Returns:
        str: Mensaje para el log, si hay una excepcion responde con el
        formato ERROR_MSG_LOG_FORMAT
    """
    exception_type, exception_value, exception_traceback = sys.exc_info()
    if not exception_type:  # If exception_type == None
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )

#------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DE EXCEPCIONES

class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


class UserError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados
    por la informacion suministrada en el event
    """

    def __init__(
        self,
        error_message="El event no tienen la estructura y/o valores esperados",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


class PlataformError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados por
    errores del sistema identificados
    """

    def __init__(
        self,
        error_message="La plataforma presenta un error, ver el log para mas detalles",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)
        
#------------------------------------------------------------------------------------------------
# CONSTANTES QUE NO SON DEL NEGOCIO

# Region por defecto para obtener el secretos desde Secrets Manager AWS
DEFAULT_SECRET_REGION = "us-east-1"
# Status codes HTML usados para dar respuesta a la solicitud de datos
STATUS_CODE_DESCRIPTIONS = {
    204: "No data",
    400: "Bad Request",
    500: "Internal Server Error",
    503: 'Service Unavailable'
}

#------------------------------------------------------------------------------------------------
# REUNE LAS FUNCIONALIDADES DE DAR RESPUESTA CON FORMATO PARA PRECIA
def create_error_response(status_code, error_message, context):
    """
    Crea la respuesta del API en caso de error, cumple el HTTP protocol version
    """
    try:
        logger.debug(
            "[precia_utils.create_error_response] Creando respuesta: error ..."
        )
        error_response = {"statusCode": status_code}
        body = {
            "error_type": STATUS_CODE_DESCRIPTIONS[status_code],
            "error_message": error_message,
        }
        stack_trace = {
            "log_group": str(context.log_group_name),
            "log_stream": str(context.log_stream_name),
            "request_id": str(context.aws_request_id),
        }
        body["stack_trace"] = stack_trace
        error_response["body"] = body
        logger.debug("[Respuesta creada.")
        return error_response
    except (Exception, ):
        log_msg = 'No se purdo crear respuesta'
        logger.critical(create_log_msg(log_msg))
        
#------------------------------------------------------------------------------------------------
# CONFIGURACIÓN PARA OBTENER SECRETOS
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Parameters:
        secret_name (str): Nombre del secreto en el servicio AWS.
        
    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = boto3.client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc
    
#------------------------------------------------------------------------------------------------
# OBTENER PARÁMETROS
def get_params(parameter_list) -> dict:
    """Obtiene los parametros de entrada del glue

    Parameters:
        parameter_list (list): Lista de parametros
    
    Returns:
        dict: Valor de los parametros
    """
    try:
        logger.info("Obteniendo parametros del glue job ...")
        params = getResolvedOptions(argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except Exception as e:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}")
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg)
        
#-------------------------------------------------------------------------------------------------

def add_time_diff_with_ny(hour:str)->str:
    ny_time_today = dt.datetime.now(tz=tz.gettz("America/New_York")).replace(
        tzinfo=None
    )
    bog_time_today = dt.datetime.now(tz=tz.gettz("America/Bogota")).replace(tzinfo=None)
    diff_time = round((ny_time_today - bog_time_today).total_seconds() / (60 * 60), 1)
    if diff_time != 0:
        hour = pd.to_datetime(hour)
        hour = hour + pd.to_timedelta(1, unit="h")
        hour = hour.time()
        hour = hour.strftime("%H:%M:%S")
    return hour


def send_warning_email(warm_msg: str):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    de una advertencia.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de advertencia"
    params_glue = get_params(["API_OPTIMUSK", "RECIPIENTS", "NOTIFICATION_ENABLED"])
    ENABLED_NOTFICATIONS = params_glue["NOTIFICATION_ENABLED"]
    API_URL = params_glue["API_OPTIMUSK"]
    mail_to = params_glue["RECIPIENTS"]

    RECIPIENTS = mail_to.replace(" ", "").split(",")
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info("Notificaciones desabilitadas en las variables de entorno")
            logger.info("No se enviara ningun correo electronico")
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
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


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
        file_size_int = int(file_size)
        if file_size_int > MAX_FILE_SIZE:
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
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
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
    params_glue = get_params(["VALUATION_DATE", "DB_STATUS_TABLE", "DB_SECRET"])
    valuation_date = params_glue["VALUATION_DATE"]
    table_status = params_glue["DB_STATUS_TABLE"]
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)
    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_connection_utils = url_sources_otc+schema_utils_otc
    db_precia_utils = DbHandler(url_connection_utils)
    connect_to_db = db_precia_utils.connect_db()
    
    try:
        schedule_name = FILE_NAME[:-15]
        status_register = {
            "id_byproduct": "swp_local",
            "name_schedule": schedule_name,
            "type_schedule": "ETL",
            "state_schedule": status,
            "details_schedule": valuation_date,
        }
        logger.info("Actualizando el status de la ETL a: %s", status)
        status_df = pd.DataFrame([status_register])
        status_df.to_sql(
            name=table_status,
            con=connect_to_db,
            if_exists="append",
            index=False,
        )
        logger.info("Status actualizado en base de datos.")
    except (Exception,) as status_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = "El ETL no puede actualizar su estatus, el proceso no sera lanzado"
        raise PlataformError(raise_msg) from status_exc
    finally:
        db_precia_utils.disconnect_db()


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
        logger.error(create_log_msg("No se pudo actualizar el status: Fallido"))
    try:
        send_error_email(error_response)
        logger.info("Error notificado")
    except (Exception,):
        log_msg = "No se pudo enviar el correo de notificacion de error"
        logger.error(create_log_msg(log_msg))


def send_error_email(error_response: dict):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    que el ETL no finalice su ejecucion correctamente.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de error"
    params_glue = get_params(["API_OPTIMUSK", "RECIPIENTS", "NOTIFICATION_ENABLED"])
    ENABLED_NOTFICATIONS = params_glue["NOTIFICATION_ENABLED"]
    API_URL = params_glue["API_OPTIMUSK"]
    mail_to = params_glue["RECIPIENTS"]

    RECIPIENTS = mail_to.replace(" ", "").split(",")
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info("Notificaciones desabilitadas en las variables de entorno")
            logger.info("No se enviara ningun correo electronico")
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
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.er(create_log_msg(error_msg))
        raise


def replace_columns_names(df_download):
    """
    Ingresa el  df que  se  dercargo en  la funcion  download_file Realiza el
    cambio de los nombres de las columnas cambiando los espacios por guion
    bajo, porteriormente realiza la limpieza de los string de las columna de
    descripcion
    :param df_replace_str: DF con los reemplazos de los strings
    :return df_replace_str: dataframe limpio
    """
    error_msg = "No fue posible cambiar el nombre de las columnas"
    try:
        logger.info("Cambiando el nombre de las columnas...")
        df_replace_str = pd.DataFrame(df_download)
        df_replace_str.columns = df_replace_str.columns.str.replace(" ", "_")
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].str.upper()
        df_replace_str = df_replace_str.replace([" BASIS"], "BASIS", regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"COP/S BASIS": " COP/S BASIS"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace([" YR", "YR"], "Y", regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"UVR/LIBOR": "UVRLIBOR", "UVR/IBR": "IBR/UVR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"UVR/LIBOR": "UVRLIBOR", "UVR/IBR": "IBR/UVR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"YRIBR": "YR IBR", "YIBR": "Y IBR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"YRUVR": "YR UVR", "YUVR": "Y UVR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"YRCOP": "YR COP", "YCOP": "Y COP"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"MIBR": "M IBR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"YRUVR": "YR UVR", "YUVR": "Y UVR"}, regex=True)
        df_replace_str["Issue_Description"] = df_replace_str[
            "Issue_Description"
        ].replace({"YRCOP": "YR COP", "YCOP": "Y COP"}, regex=True)

        df_replace_str["Vendor"] = df_replace_str["Vendor"].replace(
            ["ICP"], "ICAP", regex=True
        )
        df_replace_str = df_replace_str.replace(r"""  """, " ", regex=True)
        logger.info("Cambio de nombres de columnas exitoso")
        return df_replace_str

    except (Exception,) as rcn_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from rcn_exc


def set_diff_time():
    """
    Funcion encargada de encontrar la diferencia de tiempo entre Bogota y Nueva
    York
    :param ny_time_today: Hora Nueva York
    :param bog_time_today: Hora de Bogotá
    :return diff_time: diferencia de tiempo entra las dos ciudades
    """
    error_msg = "No se logro encontrar la diferencia horaria"
    try:
        logger.info("Se intenta encontrar la diferencia horaria entre NY y Bogotá...")
        ny_time_today = dt.datetime.now(tz=tz.gettz("America/New_York")).replace(
            tzinfo=None
        )
        bog_time_today = dt.datetime.now(tz=tz.gettz("America/Bogota")).replace(
            tzinfo=None
        )
        diff_time = round(
            (ny_time_today - bog_time_today).total_seconds() / (60 * 60), 1
        )
        logger.info("Se encontro la diferencia de tiempo")

        return diff_time
    except (Exception,) as sdt_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from sdt_exc


def set_close_hour_ny(df_replace_str):
    """
    Ingresa el df  que retorna la  funcion replace_columns_names Asignacion de
    la hora de cierre de mercado segun el horario de NY, ya sea las 13:05:00 o
    14:05:00
    :param ny_time_today: Consulta la hora de Nueva York
    :param bog_time_today: Consulta la hora de Bogota
    :param diff_time: calcula la diferencia horaria entre NY  y  Bogota
    :param close_market: define al hora de cierre de mercado
    :return df_icap: mismo df original pero con  las horas  de cierre completas
    """
    error_msg = "No fue posible asignar la hora de cierre de mercado"
    after_market = get_params(["CLOSING_HOUR"])
    after_market_close_hour = add_time_diff_with_ny(after_market["CLOSING_HOUR"])
    try:
        logger.info("Asignando hora de cierre segun el horario de NY...")
        df_replace_str["Market_Leave_Time"] = df_replace_str[
            "Market_Leave_Time"
        ].fillna(after_market_close_hour)
        df_icap = df_replace_str
        logger.info(" Asignacion de la hora de cierre exitosa")
        return df_icap
    except (Exception,) as sch_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from sch_exc


def create_new_columns(df_icap):
    """
    Ingresa el df que se completo en  la funcion set_close_hour_ny Creacion de
    las columnas: Market, date_issue,  derived_type, Underlying  y Tenor, estas
    2  ultimas  se  extraen de  la  columna   Issue_Description, order_Time,
    columna  auxiliar  para  filtrar   la  informacion  y  order, columna
    donde  se encuentra el tipo de orden
    :param df_icap_new_columns:  df  para  agregar  las  nuevas columnas
    :param df_icap_issue: variable  que  se utilizara  para  separar  lo  que
    encuentre en la columna de descripcion, para ello utilizara el  separador
    de espacio para  indicar  cuales van para Tenor y cuales para  Underlying
    :return df_icap_new_columns: df  con  las  nuevas  columnas  y  con   las
    columna Issue_Description dividida en 2 columnas
    """
    error_msg = "No fue posible crear las nuevas columnas"
    try:
        logger.info("Creando columnas nuevas...")
        df_icap_new_columns = df_icap
        df_icap_new_columns[["Market", "date_issue", "derived_type"]] = ""
        df_icap_issue = df_icap["Issue_Description"].str.split(" ", n=1, expand=True)
        df_icap_new_columns["Underlying"] = df_icap_issue[1]
        df_icap_new_columns["Tenor"] = df_icap_issue[0]
        df_icap_new_columns["order_Time"] = (
            df_icap_new_columns["Vendor"]
            + df_icap_new_columns["Issue_Description"].astype(str)
            + df_icap_new_columns["Trade_Date"].astype(str)
            + df_icap_new_columns["Order_Type"].astype(str)
            + df_icap_new_columns["Market_Entry_Time"].astype(str)
            + df_icap_new_columns["Price"].astype(str)
            + df_icap_new_columns["Amount"].astype(str)
            + df_icap_new_columns["Tenor"].astype(str)
        )
        df_icap_new_columns["order_time_temp"] = (
            df_icap_new_columns["Vendor"]
            + df_icap_new_columns["Issue_Description"].astype(str)
            + df_icap_new_columns["Trade_Date"].astype(str)
            + df_icap_new_columns["Order_Type"].astype(str)
            + df_icap_new_columns["Price"].astype(str)
            + df_icap_new_columns["Tenor"].astype(str)
        )
        df_icap_new_columns["order"] = df_icap_new_columns["Order_Type"].str.extract(
            r"\b(\w+)$", expand=True
        )
        df_icap_new_columns["order"] = df_icap_new_columns["order"].str.upper()
        df_icap_new_columns["order"] = df_icap_new_columns["order"].replace(
            "OFFER", "ASK"
        )
        logger.info("Nuevas columnas creadas exitosamente")
        return df_icap_new_columns
    except (Exception,) as cnc_exc:
        logger.debug(create_log_msg(error_msg))
        raise PlataformError from cnc_exc


def columns_to_modify(df_icap_new_columns):
    """
    Ingresa el df que retorno de la funcion create_new_columns Se realiza la
    modificacion de las columnas Underlying, Tenor y derived_type, donde en
    Tenor se realiza los  ajustes solo para  años  que  lo  que  llega como
    "Yr" sea  solo "Y", en   Underlying  asigne los nombres de las curvas y en
    derived_type, de acuerdo lo que encuentre  en  Underlying   defina  el tipo
    de  derivado,  si es  Swap  o  Forward
    :param df_columns_to_modify: Df a modificar
    :return df_columns_to_modify: df modificado
    """
    error_msg = "No fue posible modificar las columnas"
    try:
        logger.info(
            "Cambiando el contenido de las columnas para Tenor, curva y tipo de derivado..."
        )
        df_columns_to_modify = df_icap_new_columns
        df_columns_to_modify["Underlying"] = df_columns_to_modify["Underlying"].replace(
            " ", "", regex=True
        )
        df_columns_to_modify["Tenor"] = df_columns_to_modify["Tenor"].replace(
            ["YR"], "Y", regex=True
        )
        df_columns_to_modify["Underlying"] = df_columns_to_modify["Underlying"].replace(
            {
                "UVR/LIBOR": "UVRLIBOR",
                "COP/IBR": "IBR",
                "IBRFLY": "IBR",
                "COP/SBASIS": "USDCO",
                "  COP/SBASIS": "USDCO",
                "PTS": "USDCOP",
                "COP/LBASIS": "IBRLIBOR",
                "IBR/UVR": "IBUVR",
            }
        )
        df_columns_to_modify["derived_type"] = df_columns_to_modify[
            "Underlying"
        ].replace(
            {
                "UVRLIBOR (E 0.00)": "SA",
                "IBRLIBOR": "SA",
                "IBRUVR": "SA",
                "USDCO": "SA",
                "IBR": "SA",
                " IBR": "SA",
                "USDCOP": "FD",
            }
        )
        logger.info("Contenido de las columnas cambiado con exito")
        return df_columns_to_modify
    except (Exception,) as ctm_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from ctm_exc


def replace_tenor(df_columns_to_modify):
    """
    Ingresa el df que retorno de columns_to_modify Esta funcion se encarga de
    modifcar los separadores de los tenores, de tal manera que quedan
    INICIALMENTE:
    * 2YX3Y
    * 4YX5YX6Y
    Esto solo aplica para cuando llegan de a 2 y 3 tenores
    :param df_tenor_column: df a modificar
    :param df_ibr: df que solo abracara los tenores que en su curva tengan IBR
    :param df_no_ibr: df que abarcara la informacion que no contiene IBR  aqui
     entra IBRUVR
    :param df_modified_tenor, modified_tenor_ibr: df concatenados
    :return df_modified_tenor: df con tenores modificados
    """
    error_msg = "No fue posible cambiar el tenor"
    df_tenor_column = df_columns_to_modify
    try:
        logger.info("Ajustando los string de los tenores...")
        df_ibr = pd.DataFrame(df_tenor_column[df_tenor_column["Underlying"] == "IBR"])
        df_tenor_column.drop(
            df_tenor_column[(df_tenor_column["Underlying"] == "IBR")].index,
            inplace=True,
        )
        df_ibr["Tenor"] = df_ibr["Tenor"].replace(["V"], "X", regex=True)
        df_ibr["Tenor"] = df_ibr["Tenor"].replace(["VS"], "XS", regex=True)
        df_ibr["Tenor"] = df_ibr["Tenor"].replace(["XS"], "V", regex=True)
        modified_tenor_ibr = pd.concat([df_tenor_column, df_ibr])
        df_no_ibr = pd.DataFrame(
            df_tenor_column[df_tenor_column["Underlying"] != "IBR"]
        )
        df_tenor_column.drop(
            df_tenor_column[(df_tenor_column["Underlying"] != "IBR")].index,
            inplace=True,
        )
        df_no_ibr["Tenor"] = df_no_ibr["Tenor"].replace(["V"], "X", regex=True)
        df_modified_tenor = pd.concat([modified_tenor_ibr, df_no_ibr])
        df_modified_tenor["Tenor"] = df_modified_tenor["Tenor"].str.upper()
        logger.info("Ajuste de los tenores exitoso")
        return df_modified_tenor
    except (Exception,) as rt_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from rt_exc


def complete_tenor(df_modified_tenor):
    """
    Ingresa el DF que retonro de la funcion replace_tenor Funcion encargada de
    formar el tenor, se espera que los tenores queden algo asi:
    * 1Y
    * 2YX3Y
    * 4YV5YV6Y
    Se  utilizara  la  X  como  separador para dividir  los  tenores  y   poder
    completarlos, en el archivo puede llegar el tenor de manera incompleta para
    los años, finalmente se devuelve a  las V para  los  tenores que tengan mas
    de una  X, y se  eliminan las columnas creadas para armar el tenor. El
    tenor mas largo que puede llegar en el archivo  es de 11 => 11YV12YV13Y
    :param Tenor2, Tenor3, Tenor4, Tenor5, Tenor7, Tenor8 columnas  auxiliares
    para armar el tenor
    :param df_list: lista de caracteres que NO debe contener el tenor
    :param value_different: valor booleano si encuentra un tenor que tenga un
     caracter que se encuentre en df_list
    :param tenor_length_different: valor booleno  si encuentra un tenor con mas
     de 11 caracteres
    :return df_complete_tenor df con los tenores correctos
    """
    error_msg = "No fue posible formar el tenor"
    try:
        logger.info("Completando tenores...")
        df_modified_tenor["Tenor"] = df_modified_tenor["Tenor"].replace(
            ["V"], "X", regex=True
        )
        df_modified_tenor_issue = df_modified_tenor["Tenor"].str.split(
            "X", n=0, expand=True
        )
        try:
            df_modified_tenor["Tenor2"] = df_modified_tenor_issue[1]
        except Exception:
            df_modified_tenor["Tenor2"] = ""

        df_modified_tenor["Tenor"] = df_modified_tenor_issue[0]
        df_modified_tenor["Tenor3"] = df_modified_tenor["Tenor2"].str[-1]
        df_modified_tenor["Tenor3"] = df_modified_tenor["Tenor3"].fillna("")
        df_modified_tenor["Tenor4"] = df_modified_tenor["Tenor"].str[1]
        df_modified_tenor["Tenor4"] = df_modified_tenor["Tenor2"].str.isdigit()
        df_modified_tenor["Tenor4"] = df_modified_tenor["Tenor4"].map(
            {True: "", False: "Y"}
        )
        df_modified_tenor["Tenor3"] = df_modified_tenor["Tenor2"].str.isdigit()
        df_modified_tenor["Tenor3"] = df_modified_tenor["Tenor3"].map(
            {True: "Y", False: ""}
        )
        df_modified_tenor["Tenor3"] = df_modified_tenor["Tenor3"].fillna("")
        df_modified_tenor["Tenor4"] = df_modified_tenor["Tenor4"].fillna("")
        df_modified_tenor["Tenor2"] = "X" + df_modified_tenor["Tenor2"]
        df_modified_tenor["Tenor2"] = df_modified_tenor["Tenor2"].fillna("")
        df_modified_tenor["Tenor2"] = (
            df_modified_tenor["Tenor2"] + df_modified_tenor["Tenor3"]
        )
        df_modified_tenor["Tenor4"] = (
            df_modified_tenor["Tenor4"] + df_modified_tenor["Tenor3"]
        )

        try:
            df_modified_tenor["Tenor5"] = df_modified_tenor_issue[2]
            df_modified_tenor["Tenor5"] = "X" + df_modified_tenor["Tenor5"]
            df_modified_tenor["Tenor5"] = df_modified_tenor["Tenor5"].fillna("")
            df_modified_tenor["Tenor7"] = (
                df_modified_tenor["Tenor"]
                + df_modified_tenor["Tenor4"]
                + df_modified_tenor["Tenor2"]
                + df_modified_tenor["Tenor5"]
            )
        except Exception:
            df_modified_tenor["Tenor5"] = ""
            df_modified_tenor["Tenor5"] = "X" + df_modified_tenor["Tenor5"]
            df_modified_tenor["Tenor5"] = df_modified_tenor["Tenor5"].fillna("")
            df_modified_tenor["Tenor7"] = (
                df_modified_tenor["Tenor"]
                + df_modified_tenor["Tenor4"]
                + df_modified_tenor["Tenor2"]
                + df_modified_tenor["Tenor5"]
            )
        df_modified_tenor["Tenor7"] = df_modified_tenor["Tenor7"].replace(
            {"MY": "M", "MM": "M", "YY": "Y", "MYX": "MX", "VV": "X", "XX": "X"},
            regex=True,
        )
        df_modified_tenor["Tenor8"] = df_modified_tenor["Tenor7"].str.slice(-1)
        df_modified_tenor["Tenor7"] = df_modified_tenor["Tenor7"].str.slice(0, -1)
        df_modified_tenor["Tenor8"] = df_modified_tenor["Tenor8"].replace(
            {"X": ""}, regex=True
        )
        df_modified_tenor["Tenor7"] = (
            df_modified_tenor["Tenor7"] + df_modified_tenor["Tenor8"]
        )
        df_list = [
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "Z",
        ]
        regex = "|".join(df_list)
        df_modified_tenor["lettering_mistake"] = df_modified_tenor[
            "Tenor7"
        ].str.contains(regex)
        value_different = len(df_modified_tenor["lettering_mistake"].unique())
        df_modified_tenor["length_error"] = df_modified_tenor["Tenor7"].str.len()
        df_modified_tenor.loc[
            df_modified_tenor["length_error"] > 11, "tenor_length_error"
        ] = "True"
        tenor_length_different = len(df_modified_tenor["tenor_length_error"].unique())

        if (tenor_length_different == 1) & (value_different == 1):
            if value_different == 1:
                df_modified_tenor.loc[
                    df_modified_tenor["Tenor7"].str.count("X") > 1, "Tenor7"
                ] = df_modified_tenor.loc[
                    df_modified_tenor["Tenor7"].str.count("X") > 1, "Tenor7"
                ].str.replace(
                    "X", "V"
                )
                df_modified_tenor["Tenor"] = df_modified_tenor["Tenor7"]
                df_complete_tenor = df_modified_tenor.drop(
                    ["Tenor4", "Tenor2", "Tenor3", "Tenor5", "Tenor7", "Tenor8"], axis=1
                )
                logger.info("Se completaron los tenores correctamente")
                return df_complete_tenor
        else:
            df_icap_tenor_error = pd.DataFrame(
                df_modified_tenor[(df_modified_tenor["lettering_mistake"] == True)]
            )
            df_icap_tenor_error_2 = pd.DataFrame(
                df_modified_tenor[(df_modified_tenor["lettering_mistake"] == True)]
            )
            df_icap_tenor_error_total = pd.concat(
                [df_icap_tenor_error, df_icap_tenor_error_2]
            )
            logger.error(
                "Los tenores no se encuentran correctos, por favor revisar los tenores: \n"
                + str(
                    df_icap_tenor_error_total[
                        [
                            "Issue_Description",
                            "Trade_Date",
                            "Action_Type",
                            "Action_Type",
                            "Market_Leave_Time",
                            "Price",
                            "Amount",
                        ]
                    ]
                )
            )
            raise NameError
    except (Exception,) as ct_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from ct_exc


def string_replace(df_complete_tenor):
    """
    Ingresa el df que retorno de la funcion complete_tenor Funcion encargada de
    realizar  ajustes que  no  se  pudieron realizar e su respectiva funcion
    columns_to_modify, debido a que los  ajustes de IBR son diferentes a IBRUVR
    :param df_string_replace: Df con los nuevos cambios
    :retur df_string_replace
    """
    error_msg = "No fue posible realizar los ajustes a los nombres de las curvas"
    try:
        logger.info("Ajustando los nombres de las curvas y tipo de derivado...")
        df_string_replace = df_complete_tenor
        df_string_replace["derived_type"] = df_string_replace["derived_type"].replace(
            ["IBUVR", "UVR/IB3"], "IBRUVR", regex=True
        )
        df_string_replace["derived_type"] = df_string_replace["derived_type"].replace(
            ["IBRUVR"], "SA", regex=True
        )
        df_string_replace["Underlying"] = df_string_replace["Underlying"].replace(
            ["IBUVR"], "IBRUVR", regex=True
        )
        logger.info("Ajuste de nombres realizado exitosamente")
        return df_string_replace
    except (Exception,) as sr_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from sr_exc


def configure_df_formats(df_string_replace):
    """
    Ingresa el DF que retono de la funcion string_replace Funcion encargada de
    dar los formatos necesarios a las fechas y horas
    :return df_new_formats df donde se alojara el nuevo df con los formatos
    correctos
    """
    error_msg = "Revise el formato"
    try:
        logger.info("Se da inicio a los cambios de formatos...")
        df_new_formats = df_string_replace
        df_string_replace["Trade_Date"] = pd.to_datetime(
            df_string_replace["Trade_Date"], format="%d/%m/%Y"
        )
        return df_new_formats
    except (Exception,) as cdf_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from cdf_exc


def delete_cancel_actions(df_new_formats):
    """
    Esta funcion sera  llamada  por la  funcion application_filters, el
    parametro   que ingresa sera el df que retornara de la funcion
    configure_df_formats Para conocer sobre este criterio dirigirse al
    encabezado del código :return df_icap_filters df  donde  se alojara  el
    nuevo  df con la infomración eliminada segun la hora en que se presente el
    Cancel
    """
    error_msg = "No fue posible eliminar las puntas Cancel"
    after_market = get_params(["CLOSING_HOUR"])
    after_market_close_hour = add_time_diff_with_ny(after_market["CLOSING_HOUR"])
    
    before_market = get_params(["MARKET_LEAVE"])
    before_market_close_hour = add_time_diff_with_ny(before_market["MARKET_LEAVE"])
    try:
        init_msg = "Eliminando las filas con accion CANCEL aplicando"
        init_msg += " el criterio de los 5 min en el cierre de mercado..."
        logger.info(init_msg)
        df_icap_filters = df_new_formats
        df_icap_filters["Market_Leave_Time"] = df_icap_filters[
            "Market_Leave_Time"
        ].astype(str)
        df_icap_filters["Market_Entry_Time"] = df_icap_filters[
            "Market_Entry_Time"
        ].astype(str)

        df_icap_filters["Market_Entry_Time"] = df_icap_filters[
            "Market_Entry_Time"
        ].apply(pd.Timestamp)
        df_icap_filters["Market_Leave_Time"] = df_icap_filters[
            "Market_Leave_Time"
        ].apply(pd.Timestamp)

        df_icap_filters["Difference"] = (
            df_icap_filters["Market_Leave_Time"] - df_icap_filters["Market_Entry_Time"]
        ).dt.total_seconds() / 60

        df_icap_filters.loc[
            df_icap_filters["Market_Leave_Time"] <= before_market_close_hour,
            "records_to_remove",
        ] = "True"

        df_icap_filters.drop(
            df_icap_filters[
                (df_icap_filters["Action_Type"].str.contains("Insert|Modify"))
                & (df_icap_filters["order_Time"].duplicated(keep=False))
                & (
                    df_icap_filters["Order_Type"].str.contains(
                        "BID|OFFER|Managed|Subject"
                    )
                )
            ].index,
            inplace=True,
        )
        df_icap_filters.drop(
            df_icap_filters[
                (df_icap_filters["Action_Type"] == "Cancel")
                & (df_icap_filters["records_to_remove"] == "True")
            ].index,
            inplace=True,
        )
        df_icap_filters.drop(
            df_icap_filters[
                (df_icap_filters["Action_Type"] == "Cancel")
                & (df_icap_filters["Difference"] < 5.0)
                & (df_icap_filters["Market_Leave_Time"] < after_market_close_hour)
            ].index,
            inplace=True,
        )
        logger.debug("Filas con accion CANCEL correspondientes eliminadas")
        return df_icap_filters
    except (Exception,) as dca_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dca_exc


def delete_where_amount_is_0(df_new_formats):
    """
    Esta  funcion sera llamada por  la  funcion application_filters,  el
    parametro  que ingresa sera el df que retornara de la funcion
    configure_df_formats Para conocer sobre este criterio dirigirse al
    encabezado del código :return df_icap_filters df  donde  se alojara  el
    nuevo  df con la infomración eliminada segun el nominal
    """
    error_msg = "No fue posible eliminar los nominales en 0"
    try:
        logger.info("Eliminando las puntas con monto (amount) igual a 0...")
        df_icap_filters = df_new_formats
        df_icap_filters.drop(
            df_icap_filters[(df_icap_filters["Amount"] == 0)].index, inplace=True
        )
        logger.info("Puntas con monto (amount) igual a 0 eliminadas")
        return df_icap_filters
    except (Exception,) as dwa_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dwa_exc


def delete_actions_from_trades(process_df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina todas las acciones que emparejan con TRADES del dataframe dado y
    retorna un nuevo dataframe con solo los TRADES

    Args:
        process_df (pd.DataFrame): Dataframe a filtrar

    Raises:
        PlataformError: Cuando falla la eliminacion de acciones

    Returns:
        pd.DataFrame: Dataframe con TRADES
    """
    error_msg = "No fue posible traer las operaciones TRADE"
    try:
        init_msg = "Eliminando las filas de accion INSERT para las "
        init_msg += "puntas que finalizaron con una accion TRADE..."
        logger.info(init_msg)
        df_icap_filters_trade = pd.DataFrame(
            process_df[process_df["Action_Type"] == "Trade"]
        )
        df_icap_filters_trade["order"] = df_icap_filters_trade["Action_Type"]
        df_icap_filters_trade["Market_Entry_Time"].astype("string")

        for i in range(df_icap_filters_trade.index.size):
            # trade de la iteracion
            trade_row = df_icap_filters_trade.iloc[
                df_icap_filters_trade.index == df_icap_filters_trade.index[i]
            ]
            by_tenor_drop_equal = process_df[
                (
                    process_df["Issue_Description"]
                    == trade_row.Issue_Description.values[0]
                )
                & (process_df["Price"] == trade_row["Price"].values[0])
                & (process_df["Amount"] == trade_row["Amount"].values[0])
                & (
                    process_df["Market_Entry_Time"]
                    == trade_row["Market_Entry_Time"].values[0]
                )
            ]
            process_df.drop(
                index=by_tenor_drop_equal.index.values.tolist(), inplace=True
            )
            by_tenor_drop_different = process_df[
                (
                    process_df["Issue_Description"]
                    == trade_row.Issue_Description.values[0]
                )
                & (process_df["Price"] == trade_row["Price"].values[0])
                & (process_df["Amount"] == trade_row["Amount"].values[0])
                & (
                    process_df["Market_Entry_Time"]
                    == trade_row["Market_Leave_Time"].values[0]
                )
            ]
            process_df.drop(
                index=by_tenor_drop_different.index.values.tolist(), inplace=True
            )

        df_icap_filters_trade = df_icap_filters_trade.sort_values(
            ["Market_Entry_Time", "Market_Leave_Time"]
        )
        df_icap_filters_trade["order"] = df_icap_filters_trade["order"].replace(
            "Trade", "TRADE"
        )
        logger.info("Filas de accion INSERT eliminadas")
        return df_icap_filters_trade
    except (Exception,) as dia_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dia_exc


def delete_modify_actions(df_new_formats):
    """
    Esta  funcion  sera  llamada  por  la funcion application_filters, el
    parametro  que ingresa sera el df que ya se viene manipulando en dicha
    funcion Para conocer sobre este criterio dirigirse al encabezado del código
    :return df_icap_filters_2 df donde se almacenara la informacion ya con las
    puntas Modify eliminadas segun el horario
    """
    error_msg = "No fue posible eliminar las puntas Modify"
    before_market = get_params(["MARKET_LEAVE"])
    before_market_close_hour = add_time_diff_with_ny(before_market["MARKET_LEAVE"])

    after_market = get_params(["CLOSING_HOUR"])
    after_market_close_hour = add_time_diff_with_ny(after_market["CLOSING_HOUR"])
    try:
        init_msg = "Eliminando las filas con accion MODIFY aplicando"
        init_msg += " el criterio de los 5 min en el cierre de mercado..."
        logger.info(init_msg)
        df_icap_filters_2 = df_new_formats

        df_icap_filters_2["Market_Leave_Time"] = df_icap_filters_2[
            "Market_Leave_Time"
        ].astype(str)
        df_icap_filters_2["Market_Entry_Time"] = df_icap_filters_2[
            "Market_Entry_Time"
        ].astype(str)
        df_icap_filters_2["Market_Entry_Time"] = df_icap_filters_2[
            "Market_Entry_Time"
        ].apply(pd.Timestamp)
        df_icap_filters_2["Market_Leave_Time"] = df_icap_filters_2[
            "Market_Leave_Time"
        ].apply(pd.Timestamp)

        df_icap_filters_2["Difference"] = (
            df_icap_filters_2["Market_Leave_Time"]
            - df_icap_filters_2["Market_Entry_Time"]
        ).dt.total_seconds() / 60
        df_icap_filters_2.loc[
            df_icap_filters_2["Market_Leave_Time"] <= before_market_close_hour,
            "records_to_remove",
        ] = "True"

        df_icap_filters_2.drop(
            df_icap_filters_2[
                (df_icap_filters_2["Action_Type"] == "Modify")
                & (df_icap_filters_2["records_to_remove"] == "True")
            ].index,
            inplace=True,
        )
        logger.info(df_icap_filters_2.shape[0])
        df_icap_filters_2.drop(
            df_icap_filters_2[
                (df_icap_filters_2["Action_Type"] == "Modify")
                & (df_icap_filters_2["Difference"] < 5)
                & (df_icap_filters_2["Market_Leave_Time"] < after_market_close_hour)
            ].index,
            inplace=True,
        )
        logger.info(df_icap_filters_2.shape[0])
        logger.info("Filas con accion MODIFY correspondientes eliminadas")
        return df_icap_filters_2
    except (Exception,) as dma_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dma_exc


def delete_morning_actions(df_new_formats):
    """
    Esta funcion  sera  llamada por la  funcion  application_filters, el
    parametro  que ingresa sera el df que ya se viene manipulando en dicha
    funcion Para conocer sobre este criterio dirigirse al encabezado del código
    :param ny_time_today: Consulta la hora de Nueva York
    :param bog_time_today: Consulta la hora de Bogota
    :param diff_time: calcula la diferencia horaria entre NY  y Bogota
    :param condition: variable a tener en cuenta para aliminar las puntas de la
    mañana (8:00am o 9:00am)
    :param df_icap_filters_hour: df donde se alojara el df  con  los cambios de
    hora, se realiza en esta funcion, por si se presenta un modify que provenga
    de un insert que este dentro del horario a eliminar
    :return df_icap_filters: df con las puntas eliminadas segun horario
    """
    error_msg = "No se pudo eliminar las puntas que entraron en la mañana"
    market_start_hour = add_time_diff_with_ny("8:00:00")
    try:
        logger.info(
            "Eliminando los INSERTS que entraron antes de las %s...", market_start_hour
        )
        df_icap_filters = df_new_formats
        df_icap_filters["Market_Entry_Time"] = pd.to_datetime(
            df_icap_filters["Market_Entry_Time"], format="%H:%M:%S"
        ).dt.time
        df_icap_filters["Market_Leave_Time"] = pd.to_datetime(
            df_icap_filters["Market_Leave_Time"], format="%H:%M:%S"
        ).dt.time
        df_icap_filters.loc[
            df_icap_filters["Market_Entry_Time"]
            <= datetime.strptime(market_start_hour, "%X").time(),
            "Time_Verification",
        ] = "True"

        market_start_hour = datetime.strptime(market_start_hour, "%H:%M:%S").time()
        df_icap_filters_hour = pd.DataFrame(
            df_icap_filters[
                (df_icap_filters["Action_Type"] == "Modify")
                & (df_icap_filters["Time_Verification"] == "True")
            ]
        )
        df_icap_filters.drop(
            df_icap_filters[
                (df_icap_filters["Market_Entry_Time"] < market_start_hour)
                & (df_icap_filters["Action_Type"] == "Insert")
            ].index,
            inplace=True,
        )
        df_icap_filters.drop(
            df_icap_filters[
                (df_icap_filters["Market_Leave_Time"] < market_start_hour)
                & (df_icap_filters["Action_Type"] == "Cancel")
            ].index,
            inplace=True,
        )
        df_icap_filters_hour["Market_Entry_Time"] = df_icap_filters_hour[
            "Market_Leave_Time"
        ]
        df_icap_filters_hour["Market_Entry_Time"] = df_icap_filters_hour[
            "Market_Entry_Time"
        ].astype(str)
        df_icap_filters_hour["Market_Entry_Time"] = (
            df_icap_filters_hour["Market_Entry_Time"].str.replace(":", "").astype(int)
        )
        df_icap_filters_2 = pd.concat([df_icap_filters, df_icap_filters_hour])
        df_icap_filters = df_icap_filters_2
        logger.info("INSERTS que entraron antes de las %s elimnados", market_start_hour)
        return df_icap_filters
    except (Exception,) as dma_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dma_exc


def delete_subject_cancel_actions(df_new_formats):
    """
    Esta funcion sera llamada por la funcion application_filters, el parametro que ingresa
    sera el df que ya se viene manipulando en dicha funcion
    Para conocer sobre este criterio dirigirse al encabezado del código
    :return df_icap_filters: df con las puntas eliminadas segun tipo de orden  y
    accion
    """
    error_msg = "No se pudo realizar la eliminacion de la informacion"
    try:
        df_icap_filters = df_new_formats
        logger.info("Eliminando los subject CANCEL ...")
        df_icap_filters.drop(
            (
                df_icap_filters[
                    (df_icap_filters["Order_Type"].str.contains("Subject"))
                    & (df_icap_filters["Action_Type"] == "Cancel")
                ].index
            ),
            inplace=True,
        )
        logger.info("subject CANCEL eliminados")
        return df_icap_filters
    except (Exception,) as duca_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from duca_exc


def order_by_action_and_hour(process_df: pd.DataFrame) -> pd.DataFrame:
    """Ordena las filas del dataframe por el tipo de accion y por la hora
    El tipo de accion se ordena asi:
        1. INSERT
        2. MODIDY
        3. CANCEL
        4. TRADE

    Args:
        process_df (pd.DataFrame): Dataframe a ordenar

    Raises:
        PlataformError: Cuando falla el ordenamiento del dataframe

    Returns:
        pd.DataFrame: Dataframe ordenado por tipo de orden y por hora
    """
    try:
        actions_order = ["insert", "modify", "cancel", "trade"]
        order_series = pd.Series({v: i for i, v in enumerate(actions_order)})
        process_df["Market_Entry_Time"] = pd.to_datetime(
            process_df["Market_Entry_Time"]
        ).dt.time
        process_df = process_df.assign(
            Action_Type_Ordered=process_df["Action_Type"].map(order_series)
        )
        process_df = process_df.sort_values(
            by=["Action_Type_Ordered", "Market_Entry_Time"]
        )
        process_df = process_df.drop("Action_Type_Ordered", axis=1)
        process_df["Market_Entry_Time"] = process_df["Market_Entry_Time"].astype(str)
        return process_df
    except (Exception,) as ah_exc:
        logger.error(create_log_msg("Fallo el ordenamiento por tipo de accion y hora"))
        raise PlataformError from ah_exc


def replace_modify_actions(process_df: pd.DataFrame):
    """
    Reemplaza los MODIFY por INSERT + CANCEL. Si es subject MODIFY solo se
    reemplaza por CANCEL. El nuevo INSERT toma como hora de insercion la hora
    del MODIFY, y el nuevo CANCEL toma como hora de insercion la hora del INSERT
    sobre el que se hace el MODIFY y como hora de retiro la hora del MODIFY

    Args:
        process_df (pd.DataFrame): Dataframe sobre el cual se realiza el
        reemplazo de los MODIFY

    Raises:
        PlataformError: Cuando falla el reemplazo de los MODIFY

    Returns:
        pd.DataFrame: Dataframe con MODIFY sustituidos
    """
    try:
        process_df_copy = process_df.copy()
        modify_index_list = process_df_copy[
            (process_df_copy["Action_Type"] == "Modify")
        ].index.values.tolist()

        # Dataframe para guardar nuevos insert con fila dummie
        new_inserts_df = pd.DataFrame(
            {
                "Vendor": [0],
                "Issue_Description": [0],
                "Trade_Date": [0],
                "Order_Type": [0],
                "Action_Type": [0],
                "Market_Entry_Time": [0],
                "Market_Leave_Time": [0],
                "Price": [0],
                "Amount": [0],
                "Currency": [0],
                "Rejected": [0],
                "Market": [0],
                "date_issue": [0],
                "derived_type": [0],
                "Underlying": [0],
                "Tenor": [0],
                "order_Time": [0],
                "order_time_temp": [0],
                "order": [0],
                "lettering_mistake": [0],
                "length_error": [0],
                "tenor_length_error": [0],
            }
        )

        for modify_index in modify_index_list:

            moldify_row = process_df_copy.loc[modify_index]
            moldify_row_df = process_df_copy.loc[[modify_index]]

            new_insert_row_df = moldify_row_df
            new_insert_row_df.iloc[
                [0], new_insert_row_df.columns.get_loc("Action_Type")
            ] = "Insert"
            modify_type = moldify_row["Order_Type"]

            # Se agrega el nuevo INSERT si el modify si no es Subject
            if "Subject" not in modify_type:
                new_inserts_df = pd.concat([new_inserts_df, new_insert_row_df])

            # dataframe desde la primera fila hasta el modify de la iteracion
            actions_without_trades_partial_df = process_df_copy[0:modify_index]

            # precio del modify de la iteracion
            modify_price = moldify_row["Price"]

            # curva y tenores del modify de la iteracion
            modify_descripcion = moldify_row["Issue_Description"]

            is_bid = "bid" in moldify_row["Order_Type"].lower()

            # dataframe con inserts
            inserts_df = actions_without_trades_partial_df[
                (
                    actions_without_trades_partial_df["Issue_Description"]
                    == modify_descripcion
                )
                & (actions_without_trades_partial_df["Action_Type"] == "Insert")
                & (actions_without_trades_partial_df["Price"] == modify_price)
                & (
                    actions_without_trades_partial_df["Order_Type"].str.contains(
                        "bid", False
                    )
                    == is_bid
                )
            ]

            # primer insert del dataframe anterior
            one_insert_df = inserts_df.head(1)

            if not one_insert_df.empty:

                insert_init_hour = one_insert_df.loc[
                    one_insert_df.index
                    == one_insert_df["Market_Entry_Time"].index.max(),
                    "Market_Entry_Time",
                ].values[0]
                modify_init_hour = moldify_row["Market_Entry_Time"]

                # Reemplazando las horas del MODIFY
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "Market_Entry_Time",
                ] = insert_init_hour
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "Market_Leave_Time",
                ] = modify_init_hour

                # se obtiene el order type, order time y order time temp del INSERT
                insert_order_type = one_insert_df.loc[
                    one_insert_df.index == one_insert_df["Order_Type"].index.max(),
                    "Order_Type",
                ].values[0]
                insert_order_time = one_insert_df.loc[
                    one_insert_df.index == one_insert_df["order_Time"].index.max(),
                    "order_Time",
                ].values[0]
                insert_order_time_temp = one_insert_df.loc[
                    one_insert_df.index == one_insert_df["order_time_temp"].index.max(),
                    "order_time_temp",
                ].values[0]

                # Se sustituye el MODIFY por un CANCEL
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "Action_Type",
                ] = "Cancel"

                # Se sustituye el order type, order time y order time temp del
                # CANCEL por los del INSERT
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "Order_Type",
                ] = insert_order_type
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "order_Time",
                ] = insert_order_time
                process_df_copy.loc[
                    process_df_copy.index == modify_index,
                    "order_time_temp",
                ] = insert_order_time_temp

                # elimina el INSERT del que extrajo la hora para el MODIFY
                process_df_copy.drop(
                    index=one_insert_df["Market_Entry_Time"].index.max(),
                    inplace=True,
                )

        # Eliminando el primer registro dummie
        new_inserts_df = new_inserts_df.drop(new_inserts_df.index[0])

        logger.info("MODIFYs convertidos en INSERTs: %s", new_inserts_df.shape[0])

        # Agregando los nuevos registros inserts
        process_df_copy = pd.concat([process_df_copy, new_inserts_df])

        return process_df_copy
    except (Exception,) as rma_exc:
        logger.error(create_log_msg("Fallo el reemplazo de MODIFY por INSERT+CANCEL"))
        raise PlataformError from rma_exc


def count_modify_actions(process_df: pd.DataFrame) -> int:
    """Cuenta el numero de acciones MODIFY en el dataframe dado

    Args:
        process_df (pd.DataFrame): Dataframe sobre el cual se realiza el conteo

    Raises:
        PlataformError: Cuando el conteo de acciones MODIFY falla

    Returns:
        int: Numero de acciones MODIFY en el dataframe dado
    """
    try:
        number_of_modify_serie = (process_df["Action_Type"] == "Modify").value_counts()
        number_of_modify = 0
        if True in number_of_modify_serie:
            number_of_modify = number_of_modify_serie[True]
        return number_of_modify
    except (Exception,) as cma_exc:
        logger.error(create_log_msg("Fallo el conteo de acciones modify"))
        raise PlataformError from cma_exc


def filter_orders(procces_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra las puntas aplicando todos los criterios dados por el area de
    valoracion (descritos en los requerimientos de la historia de usuario).
    Filtra las acciones dejando la ultima accion de cada punta en
    representacion de la punta

    Args:
        procces_df (pd.DataFrame): Dataframe dado (antes de aplicar cualquier
        filtro)

    Raises:
        PlataformError: Cuando el filtro de puntas falla

    Returns:
        pd.DataFrame: Dataframe con solo las ultimas acciones de las puntas que
        son relevantes para la metodologia de valoracion
    """
    error_msg = "No fue posible realizar los filtros indicados"
    supr_msg = "Acciones en el dataframe despues de eliminar"
    before_market = get_params(["MARKET_LEAVE"])
    before_market_close_hour = add_time_diff_with_ny(before_market["MARKET_LEAVE"])
    
    market_close = get_params(["MARKET_CLOSE"])
    market_close_hour = add_time_diff_with_ny(market_close["MARKET_CLOSE"])
    
    market_start_hour = add_time_diff_with_ny("8:00:00")
    try:

        logger.info("Filtrando puntas...")
        actions_df = procces_df

        if procces_df.empty:
            logger.info("No hay informacion")
        else:

            logger.info(
                "Acciones en el dataframe antes de eliminar repetidos: %s",
                actions_df.shape[0],
            )
            actions_df = actions_df.drop_duplicates(keep="last")
            logger.info(
                "%s repetidos: %s",
                supr_msg,
                actions_df.shape[0],
            )

            actions_df = delete_where_amount_is_0(actions_df)
            logger.info(
                "%s puntas de monto 0: %s",
                supr_msg,
                actions_df.shape[0],
            )

            # dataframe solo con filas de acciones TRADE
            trades_df = pd.DataFrame(actions_df[actions_df["Action_Type"] == "Trade"])

            # eliminacion de trades
            actions_df.drop(
                actions_df[(actions_df["Action_Type"] == "Trade")].index,
                inplace=True,
            )
            logger.info(
                "%s TRADES: %s",
                supr_msg,
                actions_df.shape[0],
            )

            # Eliminando las acciones cancel que no apuntan a ningun insert o
            # modify
            actions_df.drop(
                actions_df[
                    (actions_df["Action_Type"] == "Cancel")
                    & (~actions_df["order_Time"].duplicated(keep=False))
                ].index,
                inplace=True,
            )

            log_msg = "%s CANCEL que no apuntan a ningun INSERT o MODIFY: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # dataframe de INSERT que emparejan con CANCEL (es decir, que
            # coinciden en tipo de orden, curva, tenores, precio, monto y hora
            # de inicio con acciones CANCEL)
            insert_from_cancel_df = actions_df[
                (actions_df["Action_Type"] == "Insert")
                & (actions_df["order_Time"].duplicated(keep=False))
            ]

            # Elimina los insert que emparejan con CANCEL
            actions_df.drop(
                insert_from_cancel_df.index,
                inplace=True,
            )
            log_msg = "%s INSERT que emparejan con CANCEL: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            order = "Managed|Subject|GTC|BID|OFFER"

            # por el momento actions_without_trades_df = actions_df, revisar a futuro
            actions_without_trades_df = pd.DataFrame(
                actions_df[actions_df["Order_Type"].str.contains(order)]
            )
            log_msg = "Acciones en el dataframe despues de filtrar por "
            log_msg += (
                "tipos de orden que contengan 'Managed|Subject|GTC|BID|OFFER': %s"
            )
            logger.info(log_msg, actions_without_trades_df.shape[0])

            # en este caso actions_df esta vacio
            actions_df.drop(
                (actions_df[(actions_df["Order_Type"].str.contains(order))].index),
                inplace=True,
            )

            # Iteracion para reemplazar modify por insert + cancel incluso
            # cuando hay modify de modify
            iterations = 0
            while True:

                number_of_modify_before = count_modify_actions(
                    actions_without_trades_df
                )
                iterations += 1
                logger.info(
                    "MODIFYs antes de sustiticion de MODIFYs numero %s: %s",
                    iterations,
                    number_of_modify_before,
                )

                before_modify_msg = "Acciones (sin TRADEs) antes de sustiticion de MODIFYs numero %s: %s"
                logger.info(
                    before_modify_msg, iterations, actions_without_trades_df.shape[0]
                )

                actions_without_trades_df = order_by_action_and_hour(
                    actions_without_trades_df
                )
                actions_without_trades_df.reset_index(drop=True, inplace=True)

                actions_without_trades_df = replace_modify_actions(
                    actions_without_trades_df
                )

                number_of_modify_after = count_modify_actions(actions_without_trades_df)
                logger.info(
                    "MODIFYs despues de sustiticion de MODIFYs numero %s: %s",
                    iterations,
                    number_of_modify_after,
                )

                # Si son iguales significa que no hubo nuevos modify validos
                # para reemplazar (modify de modify)
                if number_of_modify_before == number_of_modify_after:
                    break

            actions_df = pd.concat(
                [
                    actions_without_trades_df,
                    actions_df,
                    trades_df,
                ]
            )
            log_msg = "Acciones en el dataframe despues de "
            log_msg += "reinsertar acciones TRADEs: %s"
            logger.info(log_msg, actions_df.shape[0])

            actions_df = order_by_action_and_hour(actions_df)
            actions_df.reset_index(drop=True, inplace=True)

            # Eliminando las acciones modify que no apuntan a ningun insert o
            # modify
            actions_df.drop(
                actions_df[actions_df["Action_Type"] == "Modify"].index,
                inplace=True,
            )
            log_msg = "%s MODIFY que no apuntan a ningun INSERT o MODIFY: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los insert que coinciden en tipo de orden, curva,
            # tenores, precio, monto y hora de inicio con otros tipos de accion
            actions_df.drop(
                actions_df[
                    (actions_df["Action_Type"] == "Insert")
                    & (actions_df["order_Time"].duplicated(keep=False))
                ].index,
                inplace=True,
            )
            log_msg = "%s INSERT que empatan con otros tipos de accion: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina las acciones que empatan con TRADE y retorna otro
            # dataframe con solo los TRADES
            trades_df = delete_actions_from_trades(actions_df)
            log_msg = "%s acciones que empatan con TRADES: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            actions_df["Market_Leave_Time"] = actions_df["Market_Leave_Time"].astype(
                str
            )

            # Elimina cancel duplicados
            actions_df.drop(
                actions_df[
                    (actions_df["Action_Type"] == "Cancel")
                    & (actions_df["order_Time"].duplicated(keep="last"))
                ].index,
                inplace=True,
            )
            log_msg = "%s CANCEL reptidos: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            actions_df = delete_cancel_actions(actions_df)
            log_msg = "%s CANCEL de puntas que no cumplen el criteio de los 5 min: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los modify y cancel que estan entre las 12:55 y la 13:00
            actions_df.drop(
                actions_df[
                    (actions_df["Action_Type"].str.contains("Modify|Cancel"))
                    & (actions_df["Market_Leave_Time"] > before_market_close_hour)
                    & (actions_df["Market_Leave_Time"] <= market_close_hour)
                ].index,
                inplace=True,
            )
            log_msg = "%s CANCEL y MODIFY de puntas que estan entre las %s y las %s: %s"
            logger.info(
                log_msg,
                supr_msg,
                before_market_close_hour,
                market_close_hour,
                actions_df.shape[0],
            )

            # Elimina los insert que estan despues de la 13:00
            actions_df.drop(
                actions_df[
                    (actions_df["Action_Type"].str.contains("Insert"))
                    & (actions_df["Market_Entry_Time"] > market_close_hour)
                ].index,
                inplace=True,
            )
            log_msg = "%s INSERT que estan despues de las %s: %s"
            logger.info(
                log_msg,
                supr_msg,
                market_close_hour,
                actions_df.shape[0],
            )

            actions_df = delete_morning_actions(actions_df)
            log_msg = "%s INSERT que estan antes de las %s: %s"
            logger.info(
                log_msg,
                supr_msg,
                market_start_hour,
                actions_df.shape[0],
            )

            actions_df["order_Time"] = (
                actions_df["Vendor"]
                + actions_df["Issue_Description"].astype(str)
                + actions_df["Order_Type"].astype(str)
                + actions_df["Trade_Date"].astype(str)
                + actions_df["Price"].astype(str)
            )
            actions_df["order_Time"] = actions_df["order_Time"].replace(
                {"Managed ": "", "Subject": ""}, regex=True
            )
            actions_df["order_Time"] = actions_df["order_Time"].replace(
                {" ": ""}, regex=True
            )

            # Elimina los modify repetidos
            actions_df.drop(
                (
                    actions_df[
                        (actions_df["Action_Type"] == "Modify")
                        & actions_df["order_Time"].duplicated(keep="last")
                    ].index
                ),
                inplace=True,
            )
            log_msg = "%s MODIFY reptidos: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los cancel repetidos
            actions_df.drop(
                (
                    actions_df[
                        (actions_df["Action_Type"] == "Cancel")
                        & actions_df["order_Time"].duplicated(keep="last")
                    ].index
                ),
                inplace=True,
            )
            log_msg = "%s CANCEL reptidos: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los Managed Modify que tienen hora de entrada repetida
            actions_df.drop(
                (
                    actions_df[
                        (actions_df["Action_Type"] == "Modify")
                        & actions_df["Market_Entry_Time"].duplicated(keep=False)
                        & (actions_df["Order_Type"].str.contains("Managed"))
                    ].index
                ),
                inplace=True,
            )
            log_msg = "%s Managed MODIFY que tienen hora de entrada repetida: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los modify que son tipo bid o offer (todos?)
            actions_df.drop(
                actions_df[
                    (actions_df["Order_Type"].str.contains("BID|OFFER"))
                    & (actions_df["Action_Type"] == "Modify")
                ].index,
                inplace=True,
            )
            log_msg = "%s MODIFY que son tipo BID o OFFER: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            # Elimina los insert GTC que tienen hora de entrada repetida
            """actions_df.drop(
                (
                    actions_df[
                        (actions_df["Order_Type"].str.contains("GTC"))
                        & actions_df["Market_Entry_Time"].duplicated(keep=False)
                        & (actions_df["Action_Type"].str.contains("Insert"))
                    ].index
                ),
                inplace=True,
            )
            log_msg = "%s GTC INSERT que tienen hora de entrada repetida: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])"""

            # Elimina los cancel y modify que tienen order_time_temp repetidos
            actions_df.drop(
                (
                    actions_df[
                        (actions_df["order_time_temp"].duplicated(keep=False))
                        & actions_df["Action_Type"].str.contains("Cancel|Modify")
                    ].index
                ),
                inplace=True,
            )
            log_msg = "%s CANCEL Y MODIFY que tienen order_time_temp repetido: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            actions_df = actions_df.sort_values("Tenor")
            actions_df["order"] = actions_df["order"].str.upper()

            # Concatena el dataframe de acciones con el dataframe de TRADES en
            # un nuevo dataframe, es decir, el nuevo dataframe tiene acciones
            # TRADES duplicadas (?)
            actions_df_with_duplicated_trades = pd.concat([actions_df, trades_df])
            log_msg = "Acciones en el dataframe despues de agregar TRADEs: %s"
            logger.info(log_msg, actions_df_with_duplicated_trades.shape[0])

            # Crea un nuevo dataframe de TRADES filtrando el dataframe anterior
            duplicated_trades_df = pd.DataFrame(
                actions_df_with_duplicated_trades[
                    actions_df_with_duplicated_trades["Action_Type"] == "Trade"
                ]
            )

            # Crea un nuevo dataframe con todas las acciones excepto los trades
            without_trades_df = actions_df_with_duplicated_trades.drop(
                actions_df_with_duplicated_trades[
                    (actions_df_with_duplicated_trades["Action_Type"] == "Trade")
                ].index,
            )

            # Crea una columna auxiliar en el nuevo dataframe de TRADES
            duplicated_trades_df["order_Time2"] = (
                duplicated_trades_df["Issue_Description"].astype(str)
                + duplicated_trades_df["Order_Type"].astype(str)
                + duplicated_trades_df["Market_Entry_Time"].astype(str)
                + duplicated_trades_df["Price"].astype(str)
                + duplicated_trades_df["Amount"].astype(str)
                + duplicated_trades_df["Tenor"].astype(str)
                + duplicated_trades_df["Action_Type"].astype(str)
                + duplicated_trades_df["Market_Leave_Time"].astype(str)
            )

            # Elimina los TRADES duplicados usando la columna auxiliar
            # order_Time2 en el nuevo dataframe de TRADES (¿por que se
            # duplicaron antes?)
            new_trades_df = duplicated_trades_df.drop_duplicates(
                subset="order_Time2", keep="first"
            )

            # Organiza el dataframe que que contiene todas las acciones excepto
            # los TRADES por curva-tenor y precio
            without_trades_df = without_trades_df.sort_values(
                ["Issue_Description", "Price"], ascending=[True, True]
            )

            actions_df = pd.concat([without_trades_df, new_trades_df])
            log_msg = "%s TRADEs repetidos: %s"
            logger.info(log_msg, supr_msg, actions_df.shape[0])

            actions_df = delete_subject_cancel_actions(actions_df)
            log_msg = "Dataframe despues de eliminar los subject CANCEL: %s"
            logger.info(log_msg, actions_df.shape[0])

        logger.info("Finaliza la limpieza del archivo")
        return actions_df
    except (Exception,) as fo_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from fo_exc


def change_time_format(df_icap_filters):
    """
    ingresa el df que retorno de  application_filters
    Funcion  encargada  de  asignar  el  formato  de  la  hora  a  las  columnas
    Market_Leave_Time y  Market_Entry_Time
    :param df_icap_filters: df a cambiar el formato
    :return df_icap_time_format
    """
    error_msg = "No fue posible cambiar el fomrato de la hora"
    try:
        logger.debug("Cambiando formato de hora y fecha...")
        df_icap_filters["Action_Type"] = df_icap_filters["Action_Type"].replace(
            ["Trade"], "TRADE", regex=True
        )
        df_icap_filters["order"] = df_icap_filters["order"].replace(
            ["Trade"], "TRADE", regex=True
        )
        df_icap_filters["Market_Entry_Time"] = df_icap_filters[
            "Market_Entry_Time"
        ].astype(str)
        df_icap_filters["Market_Entry_Time"] = (
            df_icap_filters["Market_Entry_Time"].str.replace(":", "").astype(int)
        )
        df_icap_filters["Market_Entry_Time"] = pd.to_datetime(
            df_icap_filters["Market_Entry_Time"], format="%H%M%S"
        )
        df_icap_filters["Market_Entry_Time"] = [
            d.time() for d in df_icap_filters["Market_Entry_Time"]
        ]
        df_icap_filters["Market_Leave_Time"] = pd.to_datetime(
            df_icap_filters["Market_Leave_Time"], format="%H%M%S"
        )
        df_icap_filters["Market_Leave_Time"] = df_icap_filters[
            "Market_Leave_Time"
        ].astype(str)
        df_icap_filters["Market_Leave_Time"] = df_icap_filters[
            "Market_Leave_Time"
        ].str.replace("1900-01-01 ", "")
        df_icap_time_format = df_icap_filters
        logger.debug("Cambio de formato exitoso")
        return df_icap_time_format
    except (Exception,) as ctf_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from ctf_exc


def drop_columns(df_icap_time_format):
    """
    ingresa el df que retorno de  application_filters
    Funcion encargada de eliminar columnas
    :param df_icap_filters df con columnas a eliminar
    :return df_icap_to_db df listo para inserta a DB
    """
    error_msg = "No se eliminaron las columnas"
    try:
        logger.info("Se elimina la informacion que no va a la base de datos...")
        df_icap_to_db = df_icap_time_format

        df_icap_to_db = df_icap_to_db.drop(
            [
                "Currency",
                "order_Time",
                "Issue_Description",
                "Order_Type",
                "Action_Type",
                "order_time_temp",
                "Time_Verification",
                "order_Time2",
                "records_to_remove",
                "Difference",
                "lettering_mistake",
                "tenor_length_error",
                "length_error",
            ],
            axis=1,
        )
        logger.info("Se elimino la informacion correctamente")
        return df_icap_to_db
    except (Exception,) as dc_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from dc_exc


def validate_column(df_icap_to_db):
    """
    Ingreasa el df que retorno de la funcion application_filters
    Se realiza la validacion de la columna Rejected
    :param df_icap_filters df a realizar la validacion de la columna
    :return df_icap: df con infomracion a inertar en DB
    """
    error_msg = "No fue posible validar la columna Rejected"
    try:
        if "Rejected" in df_icap_to_db.columns:
            logger.info("Validacion de la columna Rejected")
            logger.debug((df_icap_to_db["Rejected"].eq("Y")).all())
            if (df_icap_to_db["Rejected"].eq("Y")).all():
                logger.debug("Continua el proceso...")
            elif "N" in df_icap_to_db["Rejected"].values:
                logger.debug("Continua el proceso...")
            elif "Y" not in df_icap_to_db["Rejected"].values:
                logger.debug(
                    "Realizando la limpieza del archivo no se encontraron puntas para insertar en la base de datos, el dataframe se encuentra vacio"
                )
                raise ValueError(
                    "Posterior a la limpieza del archivo no se encontro puntas de acuerdo con los filtros expuestos por ICAP"
                )
            else:
                records_df = df_icap_to_db[(df_icap_to_db["Rejected"] == "N")]
                records_df = records_df.index.tolist()
                logger.debug(
                    "Los indices que contienen N en la columna rejected son... "
                    + str(records_df)
                )
                logger.debug(
                    "los filtros  no contemplan ninguna fila aceptada por ICAP"
                )
                logger.debug("Se notificará el error vía correo electronico")
                logger.debug(
                    "[replace_df_names] Se notificará el error vía correo electronico"
                )
        df_icap_to_db = df_icap_to_db[(df_icap_to_db["Rejected"] == "N")]
        df_icap_to_db = df_icap_to_db.drop(
            ["Rejected"],
            axis=1,
        )
        df_icap = df_icap_to_db

        logger.info("Registros Totales a cargar en BD: " + str(df_icap.shape[0]))

        return df_icap
    except (Exception,) as vc_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError from vc_exc


def load_file_data(file_path: str, data_file) -> pd.DataFrame:
    """
    Carga los datos del archivo insumo en un DataFrame Pandas
    """
    params_glue = get_params(["VALUATION_DATE"])
    valuation_date = params_glue["VALUATION_DATE"]
    error_msg = "No se pudo transformar la informacion"
    try:
        logger.info("Extrayendo los datos del archivo como dataframe Pandas ...")
        file_extension = file_path.split(".")[-1].lower()
        if file_extension == "csv":
            data_df = pd.read_csv(io.BytesIO(b"".join(data_file)), skiprows=[0], sep=",")
            if data_df.shape[1] < len(EXPECTED_FILE_COLUMNS):
                data_df = pd.read_csv(io.BytesIO(b"".join(data_file)), skiprows=[0], sep=";")
        else:
            raise UserError(f"El archivo {file_path} no puede procesarse con este ETL.")
        logger.info("Extraccion de los datos exitosa.")
        logger.debug(
            "Datos orginales del archivo. data_df(5):\n%s", data_df.head().to_string()
        )
        data_df.dropna(axis=0, how="all", inplace=True)
        data_columns = list(data_df.columns)
        if set(data_columns) != set(EXPECTED_FILE_COLUMNS):
            raise_msg = "El archivo no tiene las columnas esperadas."
            raise_msg += (
                f" Esperadas:{EXPECTED_FILE_COLUMNS}. Encontradas: {data_columns}"
            )
            raise UserError(raise_msg)
        if data_df.empty:
            logger.warning("El DataFrame de los datos del archivo esta vacio.")
            return pd.DataFrame()
        trade_date = "Trade Date"
        if not (data_df[trade_date] == data_df[trade_date].iloc[0]).all():
            raise_msg = (
                "La columna 'Trade Date' No tiene el mismo valor en todas las filas."
            )
            raise UserError(raise_msg)
        if not (data_df["Vendor"] == data_df["Vendor"].iloc[0]).all():
            warn_msg = "La columna 'Vendor' No tiene el mismo valor en todas las filas."
            logger.warning(warn_msg)
        data_broker = data_df["Vendor"].iloc[0]
        if data_broker != BROKER:
            log_msg = "El broker del archivo no coincide con el esperado. "
            log_msg += f"Esperado: {BROKER}. Encontrado: {data_broker}"
            logger.warning(log_msg)
        file_valuation_date = data_df[trade_date].iloc[0]
        file_valuation_date = dt.datetime.strptime(
            file_valuation_date, FILE_FORMAT_DATE
        )
        data_valuation_date = file_valuation_date.strftime(ETL_FORMAT_DATE)
        if data_valuation_date != valuation_date:
            log_msg = (
                "La fecha de valoracion del archivo no coincide con la configurada. "
            )
            log_msg += (
                "Si esta en pruebas, ignore esta advertencia, en otro caso solicite "
            )
            log_msg += f"soporte. Configurada: {valuation_date}. Encontrada: {data_valuation_date}"
            logger.warning(log_msg)
            send_warning_email(log_msg)
            valuation_date = data_valuation_date
        data_df = data_df.rename({"DV01":"Amount"}, axis=1)
        logger.info("Carga de los datos del archivo exitosa.")
        logger.info("Load data: \n" + data_df.head().to_string())
        return data_df
    except UserError:
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def insert_data_to_db_swp(df_icap):
    """
    Inserta la informacion  en la tabla  src_otc_options_usdcop, de acuerdo a la
    informacion que traera el archivo que se descargo en el S3 y  se realizo  la
    limpieza de los datos
    :param df_to_swp:  Variable donde  se guardara el dataframe  que se filtrara
    teniendo en cuenta  la columna  de tipo de  derivado y asi mismo realizar la
    insercion en la respectiva tabla
    :param query_delete: Sentencia Mysql para insertar la informacion de acuerdo
    al Dataframe
    param df_to_swp_basis: Dataframe con los registros para el derivado COP/S BASIS
    :return:  Sentencia Mysql
    """
    error_msg = "No se pudo realizar la insercion de la informacion para swap"
    params_glue = get_params(["API_OPTIMUSK"])
    API_URL = params_glue["API_OPTIMUSK"]
    try:
        df_icap.rename(columns=SWP_COLUMN_DICT, inplace=True)
        df_to_swp = pd.DataFrame(df_icap[df_icap["tipo-derivado"] == "SA"])
        df_to_swp_basis = pd.DataFrame(
            df_icap[df_icap["tipo-derivado"] == "COP/S BASIS"]
        )
        df_to_swp = pd.concat([df_to_swp, df_to_swp_basis])
        df_to_swp['fecha'] = pd.to_datetime(df_to_swp['fecha'])
        df_to_swp['fecha'] = df_to_swp['fecha'].dt.strftime(ETL_FORMAT_DATE)

        df_to_swp['hora-inicial'] = df_to_swp['hora-inicial'].apply(lambda x: str(x))
        df_to_swp['hora-final'] = df_to_swp['hora-final'].apply(lambda x: str(x))
        if df_to_swp.empty:
            logger.info("No hay informacion para swaps")
        else:
            df_to_swp.reset_index(inplace=True, drop=True)
            data = []
            for index, row in df_to_swp.iterrows():
                registro = {
                    "sistema": row["sistema"],
                    "tipo-derivado": row["tipo-derivado"],
                    "mercado": row["mercado"],
                    "subyacente": row["subyacente"],
                    "fecha": row["fecha"],
                    "hora-inicial": row["hora-inicial"],
                    "hora-final": row["hora-final"],
                    "fecha-emision": row["fecha-emision"],
                    "tenor": row["tenor"],
                    "nominal": row["nominal"],
                    "tipo-orden": row["tipo-orden"],
                    "precio": row["precio"],
                }
                data.append(registro)

            df_to_swp_db = {"data": data}
            logger.info(f"Información a insertar {df_to_swp_db}")
            url_api_insert_swp_local = f"{API_URL}/src/otc/swap/local"
            mail_response = requests.post(url_api_insert_swp_local, json=df_to_swp_db, timeout=10)
            api_mail_code = str(mail_response.status_code)
            api_mail_text = mail_response.text
            logger.info(
                "Respuesta del servicio de notificaciones para la inserción: code: %s, body: %s",
                api_mail_code,
                api_mail_text,
            )
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise

#-------------------------------------------------------------------------------------------------
# DESCARGAR FTP
class SFTPHandler:
    """
    Representa el lector del archivo del SFTP
    Envía los archivos creados al servidor
    """
    
    def __init__(self, data_connection):
        self.data_connection = data_connection
        

    def connect_to_sftp(self, timeout: int = 40) -> paramiko.SSHClient:
        """
        Se conecta a un SFTP usando las credenciales contenidas en 'secret'
        y la libreria 'paramiko'. Las credenciales 'secret' se almacenan en Secrets Manager
    
        Returns:
            paramiko.SSHClient: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
        """
        raise_msg = "No fue posible conectarse al SFTP destino"
        try:
            logger.info("Conectandose al SFTP ...")
            logger.info("Validando secreto del SFTP ...")
            sftp_host = self.data_connection["host"]
            sftp_port = self.data_connection["port"]
            sftp_username = self.data_connection["username"]
            sftp_password = self.data_connection["password"]
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
            self.ssh_client.connect(
                sftp_host,
                port=sftp_port,
                username=sftp_username,
                password=sftp_password,
                timeout=timeout,
            )
            logger.info("Conexion al SFTP exitosa.")
            self.sftp = self.ssh_client.open_sftp()
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(raise_msg) from unknown_exc

    def disconnect_sftp(self):
        """Cierra la conexión y la sesión hacia el SFTP"""
        if self.sftp is not None:
            self.sftp.close()
            self.ssh_client.close()
            logger.info("Se ha cerrado la conexion al SFTP")
        else:
            logger.info("No hay conexion al servidor SFTP")
            
            
    def get_file(self, path_file: str):
        """
        Obtiene archivos desde un FTP en la ruta proporcionada
        """
        try:
            logger.info("Descargando archivos del FTP %s ...")
            self.connect_to_sftp()
            self.sftp.stat(path_file)
            with self.sftp.open(path_file, "rb") as remote_file:
                file_data = remote_file.readlines()
            logger.info("Se ha descargado el archivo correctamente del FTP")
            file_size = self.sftp.stat(path_file).st_size
            return file_data, file_size
        except FileNotFoundError as error:
            error_msg = f"El archivo {path_file} no existe en el FTP"
            logger.error(create_log_msg(error_msg))
            raise FileNotFoundError(error_msg) from error
        except (Exception,) as e:
            logger.error(
                create_log_msg(
                    "Ocurrio un error en la descarga del archivo desde el FTP"
                )
            )
            raise PlataformError("No se pudo descargar el archivo del FTP") from e
        finally:
            self.disconnect_sftp()


def check_business_day(valuation_date_str: str) -> bool:
    """
    Revisa que el dia de valoracion sea un dia habil

    Args:
        valuation_date_str (str): Fecha habil configurada en el secreto de configuracion

    Raises:
        PlataformError: Cuando la API de calendarios de Precia no responde satisfactoriamente

    Returns:
        bool: True si es dia habil y False si NO es dia habil
    """
    error_log = "No fue posible verificar si es dia habil"
    try:
        params_glue = get_params(["API_OPTIMUSK"])
        API_URL = params_glue["API_OPTIMUSK"]
        logger.info("Verificando si la fecha de valoracion es un dia habil ...")
        run_date = datetime.strptime(valuation_date_str, ETL_FORMAT_DATE)
        init_date = run_date - rd.relativedelta(days=3)
        end_date = run_date + rd.relativedelta(days=3)
        init_date_str = datetime.strftime(init_date, ETL_FORMAT_DATE)
        end_date_str = datetime.strftime(end_date, ETL_FORMAT_DATE)
        calendar_url = f"{API_URL}/utils/calendars?"
        calendar_url += f"campos=CO&fecha-inicial={init_date_str}&fecha-final={end_date_str}"
        logger.info("URL API: %s", calendar_url)
        api_response = requests.get(calendar_url, timeout=10)
        api_response_code = api_response.status_code
        api_response_data = api_response.json()
        api_response_msg = (
            f"code: {api_response_code}, body: {api_response_data}."
        )
        logger.info("Repuesta del API: %s", api_response_msg)
        if api_response_code != 200:
            log_msg = " La respuesta del API contiene un codigo de error"
            logger.error(create_log_msg(log_msg))
            raise_msg = (
                "El API de calendarios no respondio satisfactoriamente a la solicitud"
            )
            raise PlataformError(raise_msg)
        calendar_data = api_response_data["data"]
        if valuation_date_str not in calendar_data:
            log_msg = f"La fecha de valoracion {valuation_date_str} NO es un dia habil"
            logger.error(create_log_msg(log_msg))
            return False
        log_msg = f"La fecha de valoracion {valuation_date_str} es un dia habil"
        logger.info(log_msg)
        return True
    except (Exception,):
        logger.error(create_log_msg(error_log))
        raise
   
   
class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str) -> None:
        self.connection = None
        self.url_db = url_db
        
    
    def connect_db(self):
        """Genera la conexión a base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            return self.connection
        
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise PlataformError("Hubo un error en la conexión a base de datos: " + str(e))


    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None      
            

def main():
    """
    Orquesta la ejecución de la ETL
    """
    params_glue = get_params(["VALUATION_DATE", "FTP_SECRET"])
    valuation_date = params_glue["VALUATION_DATE"]
    error_msg = "Hubo un error en la ejecución del glue"
    is_business_day = check_business_day(valuation_date)
    valuation_date_file = valuation_date.replace("-", "")
    file_name_tp = FILE_NAME+"_"+valuation_date_file+".csv"
    if not is_business_day: 
        logger.info("Hoy no es un día hábil, no se recolecta el archivo de Tullet")
        return False
    else:
        logger.info("Hoy es un día hábil, recolecta el archivo y se corre la ETL")
        try:
            secret_sftp = params_glue["FTP_SECRET"]
            key_secret_sftp = get_secret(secret_sftp)
            data_connection_sftp = {
                "host": key_secret_sftp["sftp_host"],
                "port": key_secret_sftp["sftp_port"],
                "username": key_secret_sftp["sftp_user"],
                "password": key_secret_sftp["sftp_password"],
            }
            route_tullet = key_secret_sftp["route_tullet"]+file_name_tp
            loader = SFTPHandler(data_connection_sftp)
            data_file_tullet, file_size = loader.get_file(route_tullet)
            is_empty_file = validate_file_structure(file_size)
            if is_empty_file:
                update_status("Exitoso")
                body = "Finaliza la ejecucion exitosamente por que el archivo esta vacio"
                send_warning_email(body)
                response = {"statusCode": 200, "body": body}
                logger.info(body)
                return response
            raw_file_data_df = load_file_data(route_tullet, data_file_tullet)
            # Inicio de la lógica de negocio
            raw_file_data_df = raw_file_data_df[(raw_file_data_df["Rejected"] == "N")]
            df_replace_str = replace_columns_names(raw_file_data_df)
            df_icap = set_close_hour_ny(df_replace_str)
            df_icap_new_columns = create_new_columns(df_icap)
            df_columns_to_modify = columns_to_modify(df_icap_new_columns)
            df_modified_tenor = replace_tenor(df_columns_to_modify)
            df_complete_tenor = complete_tenor(df_modified_tenor)
            df_string_replace = string_replace(df_complete_tenor)
            df_new_formats = configure_df_formats(df_string_replace)
            df_icap_filters = filter_orders(df_new_formats)
            df_icap_to_db = drop_columns(df_icap_filters)
            df_icap = validate_column(df_icap_to_db)
            logger.info("Insertando informacion de swaps local...")
            logger.info(f"Información a insertar: {df_icap.to_string()}")
            insert_data_to_db_swp(df_icap)
            logger.info("Informacion de swaps local insertada con exito")
            update_status("Exitoso")
        except (PlataformError, UserError):
            logger.critical(create_log_msg(error_msg))
        except (Exception,):
            logger.critical(create_log_msg(error_msg))
        
if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main()
