# Nativas de Python
import os
import logging
from json import loads as json_loads, dumps as json_dumps
import datetime as dt
from dateutil import relativedelta as rd
from sys import argv, exc_info, stdout
from base64 import b64decode

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client

# De terceros
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, engine
from sqlalchemy.sql import text
import paramiko
import numpy as np
from scipy import optimize

pd.options.mode.chained_assignment = None
first_error_msg = None


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


# EXCEPCIONES PERSONALIZADAS: INICIO--------------------------------------------


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


# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------


# CLASE CONEXION DB: INICIO-----------------------------------------------------
class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""

    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.connection = None
        self.valuation_date = valuation_date
        self.url_db = url_db

    def connect_db(self):
        """Genera la conexión a base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            logger.info("Se conecto correctamente a la base de datos")
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise PlataformError(
                "Hubo un error en la conexión a base de datos: " + str(e)
            )

    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None


# CLASE CONEXION DB: FIN--------------------------------------------------------
# OBTENCION DE SECRETOS:
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
        "DATANFS_SECRET"
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


class HzdlFiles:
    def __init__(self):
        params_dict = GlueManager.get_params()
        self.db_secret= SecretsManager.get_secret(params_dict["DB_SECRET"])
        self.datanfs_secret = SecretsManager.get_secret(params_dict["DATANFS_SECRET"])
        self.utils_engine = create_engine(
            self.db_secret["conn_string_aurora_publish"] + self.db_secret["schema_aurora_publish"]
        )

    def run(self):
        date_today = dt.datetime.now().date().strftime("%Y%m%d")
        select_query = text(
            f"""
            SELECT id_precia,days,pd_value,ps_value\
            FROM pub_otc_probabilities WHERE status_info = 1\
            AND id_precia IN ("BAAA2", "FAAA2", "RAAA3", "F60", "F59", "F58", "B60", "B59", "B58", "R60", "R59", "R50", "R40");
            """
        )
        request = pd.read_sql(select_query, con=self.utils_engine)
        sp_all = request[["id_precia", "days", "ps_value"]].copy()
        pd_all = request[["id_precia", "days", "pd_value"]].copy()
        sp_groups = sp_all.groupby("id_precia")
        pd_groups = pd_all.groupby("id_precia")

        select_query = text(
            f"""
            SELECT id_precia,days,rate\
            FROM pub_otc_hazzard_rates WHERE status_info = 1\
            AND id_precia IN ("BAAA2", "FAAA2", "RAAA3", "F60", "F59", "F58", "B60", "B59", "B58", "R60", "R59", "R50", "R40");
            """
        )

        request_hazzard = pd.read_sql(select_query, con=self.utils_engine)
        hzd_all = request_hazzard[["id_precia", "days", "rate"]].copy()
        hzd_groups = hzd_all.groupby("id_precia")

        for name, group in sp_groups:
            file_name = f"Sp_{name}_Nodos_{date_today}.csv"
            group = group[["days", "ps_value"]]
            group.rename(columns={"ps_value": "sp"}, inplace=True)
            group.to_csv(file_name, index=False)
            self.create_and_transfer_file(
                group, file_name, self.datanfs_secret["route_pds"]
            )

        for name, group in pd_groups:
            file_name = f"Pd_{name}_Nodos_{date_today}.csv"
            group = group[["days", "pd_value"]]
            group.rename(columns={"pd_value": "pd"}, inplace=True)
            self.create_and_transfer_file(
                group, file_name, self.datanfs_secret["route_pds"]
            )

        for name, group in hzd_groups:
            file_name = f"Haz_{name}_Nodos_{date_today}.csv"
            group = group[["days", "rate"]]
            group.rename(columns={"rate": "hazzard"}, inplace=True)
            self.create_and_transfer_file(
                group, file_name, self.datanfs_secret["route_hazzard"]
            )

    def create_and_transfer_file(
        self, curve_df: pd.DataFrame, filename: str, route: str
    ) -> None:
        try:
            logger.info(
                "Creando y transfiriendo el archivo '%s' al datanfs...",
                filename,
            )
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=self.datanfs_secret["sftp_host"],
                port=self.datanfs_secret["sftp_port"],
                username=self.datanfs_secret["sftp_user"],
                password=self.datanfs_secret["sftp_password"],
                timeout=60,
            )
            with ssh_client.open_sftp().open(
                f"{route}{filename}",
                "w",
            ) as file:
                file.write(curve_df.to_csv(index=False))
            logger.info(
                "Creacion y transferencia del archivo al datanfs exitosa",
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion y transferencia de archivos"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


if __name__ == "__main__":
    data = HzdlFiles()
    data.run()
