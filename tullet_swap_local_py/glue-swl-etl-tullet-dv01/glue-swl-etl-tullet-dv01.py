from datetime import datetime
from botocore.client import Config
import boto3
from json import loads as json_loads, dumps as json_dumps
import io

import pandas as pd
import requests
from sqlalchemy import create_engine, engine
import logging
from sys import argv, exc_info, stdout
import paramiko
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client
from base64 import b64decode
from sqlalchemy.pool import NullPool

COLUMN_DICT = {
    "Fecha": "date_dv01",
    "Curva": "instrument_dv01",
    "Tenor": "tenor_dv01",
    "DV01": "dv01",
}
FILE_NAME = "DV01_TP"
BROKER = "TP"
EXPECTED_FILE_COLUMNS = ["Fecha", "Curva", "Tenor", "DV01"]
MAX_FILE_SIZE = 5e7
ETL_FORMAT_DATE = "%Y-%m-%d"
FILE_FORMAT_DATE = "%d/%m/%Y"
pd.options.display.float_format = "{:20,.2f}".format
VALUATION_DATE = "2024-04-14"

# LOGGER: INICIO --------------------------------------------------------------
ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)


def setup_logging(log_level):
    """
    formatea todos los logs que invocan la libreria logging
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = logging.StreamHandler(stdout)
    precia_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
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
    exception_type, exception_value, exception_traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )


logger = setup_logging(logging.INFO)

# LOGGER: FIN -----------------------------------------------------------------

# ------------------------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------------------
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


# -------------------------------------------------------------------------------
class SecretsManager:
    @staticmethod
    def get_secret(secret_name: str) -> dict:
        """
        Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

        Args:
            secret_name (str): Nombre del secreto en el servicio AWS.

        Raises:
            PlataformError: Cuando ocurre algun error al obtener el secreto.

        Returns:
            dict: Secreto con la informacion desplegada en Secrets Manager AWS.
        """
        try:
            logger.info('Intentando obtener secreto: "%s" ...', secret_name)
            cliente_secrets_manager = aws_client("secretsmanager")
            secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
            if "SecretString" in secret_data:
                secret_str = secret_data["SecretString"]
            else:
                secret_str = b64decode(secret_data["SecretBinary"])
            logger.info("Se obtuvo el secreto.")
            return json_loads(secret_str)
        except (Exception,) as sec_exc:
            error_msg = f'Fallo al obtener el secreto "{secret_name}"'
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from sec_exc


# CREACION DE ARCHIVOS: INICIO--------------------------------------------------
# OBTENER PARAMETROS: INICIO----------------------------------------------------
class GlueManager:
    PARAMS = [
        "DB_SECRET",
        "FTP_SECRET",
        "VALUATION_DATE",
        "API_OPTIMUSK",
        "RECIPIENTS",
        "NOTIFICATION_ENABLED",
        "DV01_SCALES",
    ]

    @staticmethod
    def get_params() -> dict:
        """Obtiene los parametros de entrada del glue

        Raises:
            PlataformError: Cuando falla la obtencion de parametros

        Returns:
            tuple:
                params: Todos los parametros excepto los nombres de las lambdas
                lbds_dict: Nombres de las lambdas de metodologia
        """
        try:
            logger.info("Obteniendo parametros del glue job ...")
            params = getResolvedOptions(argv, GlueManager.PARAMS)
            logger.info("Obtencion de parametros del glue job exitosa")
            logger.debug("Parametros obtenidos del Glue:%s", params)
            return params
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la obtencion de los parametros del job de glue"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


# OBTENER PARAMETROS: FIN--------------------------------------------------------


class processdv01:

    def __init__(self) -> None:

        params_dict = GlueManager.get_params()
        self.db_secret = SecretsManager.get_secret(params_dict["DB_SECRET"])
        self.FTP_SECRET = SecretsManager.get_secret(params_dict["FTP_SECRET"])
        self.utils_engine = create_engine(
            self.db_secret["conn_string_aurora_sources"]
            + self.db_secret["schema_aurora_precia_utils"],
            poolclass=NullPool,
        )
        self.valuation_date = params_dict["VALUATION_DATE"]
        self.ENABLED_NOTFICATIONS = params_dict["NOTIFICATION_ENABLED"]
        self.API_URL = params_dict["API_OPTIMUSK"]
        self.mail_to = params_dict["RECIPIENTS"]
        self.DV01_SCALES = json_loads(params_dict["DV01_SCALES"])

    def update_status(self, status):
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
                name="precia_utils_schedules_status",
                con=self.utils_engine,
                if_exists="append",
                index=False,
            )
            logger.info("Status actualizado en base de datos.")
        except (Exception,) as status_exc:
            logger.critical(create_log_msg(error_msg))
            raise_msg = (
                "El ETL no puede actualizar su estatus, el proceso no sera lanzado"
            )
            raise PlataformError(raise_msg) from status_exc

    def send_warning_email(self, warm_msg: str):
        """
        Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
        de una advertencia.
        :param recipients, lista de los correos electronicos donde llegara la notificacion de error
        :param error_response, diccionario con el mensaje de error que se quiere enviar
        """
        error_msg = "No se pudo enviar el correo con la notificacion de advertencia"

        RECIPIENTS = self.mail_to.replace(" ", "").split(",")
        try:
            if not eval(self.ENABLED_NOTFICATIONS):
                logger.info("Notificaciones desabilitadas en las variables de entorno")
                logger.info("No se enviara ningun correo electronico")
                return None
            logger.info("Creando email de advertencia ... ")
            url_notification = f"{self.API_URL}/utils/notifications/mail"
            logger.info("URL API de notificacion: %s", url_notification)
            subject = f"Optimus-K: Advertencia durante el procesamiento del archivo {FILE_NAME}"
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
                raise_msg = "El API de notificaciones por email no respondio satisfactoriamente."
                raise PlataformError(raise_msg)
            logger.info("Email enviado.")
            return None
        except PlataformError:
            logger.error(create_log_msg(error_msg))
            raise
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def validate_file_structure(self, file_size: str) -> bool:
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
                    "El archivo %s es sospechosamente grande, no sera procesado.",
                    FILE_NAME,
                )
                raise_msg = (
                    f"El archivo {FILE_NAME} supera el tamanio maximo aceptado. "
                )
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

    def load_file_data(self, file_path: str, data_file) -> pd.DataFrame:
        """
        Carga los datos del archivo insumo en un DataFrame Pandas
        """
        error_msg = "No se pudo transformar la informacion"
        try:
            logger.info("Extrayendo los datos del archivo como dataframe Pandas ...")
            file_extension = file_path.split(".")[-1].lower()
            if file_extension == "csv":
                data_df = pd.read_csv(io.BytesIO(b"".join(data_file)), sep=",")
                if data_df.shape[1] < len(EXPECTED_FILE_COLUMNS):
                    data_df = pd.read_csv(io.BytesIO(b"".join(data_file)), sep=";")
            else:
                raise UserError(
                    f"El archivo {file_path} no puede procesarse con este ETL."
                )
            logger.info("Extraccion de los datos exitosa.")
            logger.info(
                "Datos orginales del archivo. data_df(5):\n"
                + data_df.head().to_string()
            )
            data_df.dropna(axis=0, how="all", inplace=True)
            data_df.dropna(axis=1, how="all", inplace=True)
            data_df = data_df.loc[:, ~data_df.columns.str.contains("^Unnamed")]
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
            if not (data_df["Fecha"] == data_df["Fecha"].iloc[0]).all():
                raise_msg = (
                    "La columna 'Fecha' No tiene el mismo valor en todas las filas."
                )
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
            data_df["date_dv01"] = pd.to_datetime(
                data_df["date_dv01"], format=FILE_FORMAT_DATE
            )
            data_df["date_dv01"] = data_df["date_dv01"].dt.strftime(ETL_FORMAT_DATE)
            data_valuation_date = data_df["date_dv01"].iloc[0]
            if data_valuation_date != self.valuation_date:
                log_msg = "La fecha de valoracion del archivo no coincide con la configurada. "
                log_msg += "Si esta en pruebas, ignore esta advertencia, en otro caso solicite "
                log_msg += f"soporte. Configurada: {self.valuation_date}. Encontrada: {data_valuation_date}"
                logger.warning(log_msg)
                self.send_warning_email(log_msg)
                self.valuation_date = data_valuation_date
            logger.info(
                "Datos limpiados del archivo. data_df(5):\n"
                + data_df.head().to_string()
            )
            data_df["dv01"] = pd.to_numeric(data_df["dv01"])  # , downcast='float')
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

    def scale_dv01(self, unscaled_df: pd.DataFrame):
        """
        Aplica un escalamiento a todos los valores de DV01 del dataframe,
        según las escalas indicadas en el diccionario 'dv01_scales'
        Args:
            unscaled_df (pd.DataFrame): dataframe con los valores de DV01
                originales
        Returns:
            pd.DataFrame: dataframe con todos los escalamientos aplicados
        """
        error_msg = "Fallo el escalamiento de los valores de DV01"
        try:
            scaled_df = unscaled_df
            logger.info("BROKER: %s", BROKER)
            logger.debug("unscaled dataframe:\n%s", unscaled_df)
            broker_dv01_scales = self.DV01_SCALES[BROKER]
            logger.info("broker_dv01_scales:\n%s", broker_dv01_scales)
            logger.info("Unscaled DV01: \n" + unscaled_df.to_string())
            for instrument in broker_dv01_scales:
                logger.debug("scale: %s", broker_dv01_scales[instrument])
                scaled_df.loc[unscaled_df["instrument_dv01"] == instrument, "dv01"] = (
                    unscaled_df["dv01"] * broker_dv01_scales[instrument]
                )
            logger.info("Scaled DV01: \n" + scaled_df.to_string())
            logger.debug("scaled dataframe:\n%s", scaled_df)
            return scaled_df
        except (Exception,) as scl_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from scl_exc

    def update_database(self, data_df: pd.DataFrame):
        error_msg = "Fallo el cargue de informacion desde la API"
        try:
            data_df.reset_index(inplace=True, drop=True)
            data = []
            for index, row in data_df.iterrows():
                registro = {
                    "fecha": row["date_dv01"],
                    "instrumento": row["instrument_dv01"],
                    "tenor": row["tenor_dv01"],
                    "dv01": row["dv01"],
                    "sistema": row["systems"],
                }
                data.append(registro)

                df_to_swp_db = {"data": data}
                logger.info(f"Información a insertar {df_to_swp_db}")
                url_api_insert_swp_local = f"{self.API_URL}/src/otc/swap/local/dv01"
                api_response = requests.post(
                    url_api_insert_swp_local, json=df_to_swp_db, timeout=10
                )
                api_mail_code = str(api_response.status_code)
                api_mail_text = api_response.text
                logger.info(
                    "Respuesta del servicio de notificaciones para la inserción: code: %s, body: %s",
                    api_mail_code,
                    api_mail_text,
                )
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def run(self):
        """
        Orquesta la ejecución de la ETL
        """
        error_msg = "Hubo un error en la ejecución del glue"
        valuation_date_file = self.valuation_date.replace("-", "")
        global FILE_NAME
        FILE_NAME = FILE_NAME + "_" + valuation_date_file + ".csv"
        try:
            key_secret_sftp = self.FTP_SECRET
            data_connection_sftp = {
                "host": key_secret_sftp["sftp_host"],
                "port": key_secret_sftp["sftp_port"],
                "username": key_secret_sftp["sftp_user"],
                "password": key_secret_sftp["sftp_password"],
            }
            route_tullet = key_secret_sftp["route_tullet"] + FILE_NAME
            loader = SFTPHandler(data_connection_sftp)
            data_file_tullet, file_size = loader.get_file(route_tullet)
            is_empty_file = self.validate_file_structure(file_size)
            if is_empty_file:
                self.update_status("Exitoso")
                body = (
                    "Finaliza la ejecucion exitosamente por que el archivo esta vacio"
                )
                self.send_warning_email(body)
                response = {"statusCode": 200, "body": body}
                logger.info(body)
                return response
            raw_file_data_df = self.load_file_data(route_tullet, data_file_tullet)
            raw_file_data_df = self.scale_dv01(raw_file_data_df)
            self.update_database(raw_file_data_df)
            self.update_status("Exitoso")
            logger.info("Finaliza la ejecucion exitosamente!")
        except (Exception,):
            logger.critical(create_log_msg(error_msg))


if __name__ == "__main__":
    data = processdv01()
    data.run()
