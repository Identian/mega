# Nativas de Python
import os
import logging
from json import loads as json_loads, dumps as json_dumps
from datetime import datetime as dt, timedelta
from dateutil import relativedelta as rd
from sys import argv, exc_info, stdout
from base64 import b64decode
from smtplib import SMTP
from email.message import EmailMessage

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client


# De terceros
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, engine
from sqlalchemy.sql import text
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
    PARAMS = ["DB_SECRET", "VALUATION_DATE", "CURVE_NAME", "NODES", "MAIL_SECRET"]

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
# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------


class ReportEmail:
    """
    Clase que representa los correos que reportan las variaciones del día

    Attributes:
    ----------
    subject: str
        Asunto del correo
    body: str
        Cuerpo del correo
    data_file: dict
        Información para el archivo adjunto del correo

    Methods:
    --------
    connect_to_smtp(secret)
        Conecta al servicio SMTP de precia

    create_mail_base
        Crea el objeto EmailMessage con los datos básicos para enviar el correo

    attach_file_to_message(message, df_file)
        Adjunta el archivo al mensaje para enviar por el correo

    send_email(smtp_connection, message)
        Envia el objeto EmailMessage construido a los destinatarios establecidos

    run()
        Orquesta los métodos de la clase

    """

    def __init__(self, subject, body, data_file=None) -> None:
        self.subject = subject
        self.body = body
        self.data_file = data_file

    def connect_to_smtp(self, secret):
        """Conecta al servicio SMTP de precia con las credenciales que vienen en secret
        Parameters:
        -----------
        secret: dict, required
            Contiene las credenciales de conexión al servicio SMTP

        Returns:
        --------
        Object SMTP
            Contiene la conexión al servicio SMTP
        """
        error_msg = "No fue posible conectarse al relay de correos de Precia"
        try:
            logger.info("Conectandose al SMTP ...")
            connection = SMTP(host=secret["server"], port=secret["port"])
            connection.starttls()
            connection.login(secret["user"], secret["password"])
            logger.info("Conexión exitosa.")
            return connection
        except (Exception,) as url_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from url_exc

    def create_mail_base(self, mail_from, mails_to):
        """Crea el objeto EmailMessage que contiene todos los datos del email a enviar
        Parameters:
        -------
        mail_from: str, required
            Dirección de correo que envía el mensaje
        mails_to: str, required
            Direcciones de correo destinatario

        Returns:
        Object EmailMessage
            Contiene el mensaje base (objeto) del correo

        Raises:
        ------
        PlataformError
            Si no se pudo crear el objeto EmailMessage

        """
        error_msg = "No se crear el correo para el SMTP"
        try:
            logger.info('Creando objeto "EmailMessage()" con los datos básicos...')
            message = EmailMessage()
            message["Subject"] = self.subject
            message["From"] = mail_from
            message["To"] = mails_to
            message.set_content(self.body)
            logger.info("Mensaje creado correctamente")
            return message
        except (Exception,) as mail_exp:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from mail_exp

    def send_email(self, smtp_connection, message):
        """Envia el objeto EmailMessage construido a los destinatarios establecidos
        Parameters:
        -----------
        smtp_connection: Object SMTP, required
            Contiene la conexión con el servicio SMTP
        message: Object EmailMessage
            Mensaje para enviar por correo

        Raises:
        -------
        PlataformError
            Si no pudo enviar el correo
            Si no pudo cerrar la conexión SMTP
        """
        logger.info("Enviando Mensaje...")
        try:
            smtp_connection.send_message(message)
        except (Exception,) as conn_exc:
            logger.error(create_log_msg("No se pudo enviar el mensaje"))
            raise PlataformError("No se pudo enviar el mensaje") from conn_exc
        finally:
            try:
                smtp_connection.quit()
                logger.info("Conexión cerrada: SMTP")
            except (Exception,) as quit_exc:
                logger.error(create_log_msg("No se pudo cerra la conexión"))
                raise PlataformError(
                    "Hubo un error cerrando la conexión SMTP"
                ) from quit_exc


def send_email(body: str, smtp_secret: dict, subject: str) -> None:
    """Envia un correo

    Args:
        body (str): Cuerpo del correo a enviar
        smtp_secret (dict): Informacion necesaria para conectarse al
        servidor SMTP y enviar el correo
        subject (str): Asunto del correo

    Raises:
        PlataformError: Cuando falla el envio del correo
    """
    logger.debug("smtp_secret:\n%s", smtp_secret)
    smpt_credentials = {
        "server": smtp_secret["smtp_server"],
        "port": smtp_secret["smtp_port"],
        "user": smtp_secret["smtp_user"],
        "password": smtp_secret["smtp_password"],
    }
    mail_from = smtp_secret["mail_from"]
    mail_to = smtp_secret["mail_to"]
    # Eliminando las tabulaciones de la identacion de python
    body = "\n".join(line.strip() for line in body.splitlines())
    email = ReportEmail(subject, body)
    smtp_connection = email.connect_to_smtp(smpt_credentials)
    message = email.create_mail_base(mail_from, mail_to)
    email.send_email(smtp_connection, message)


class swapvalidator:
    def __init__(self):
        self.params = SecretsManager.get_secret(params_dict["DB_SECRET"])
        self.smtp_secret = get_secret(params_dict["MAIL_SECRET"])
        params_dict = GlueManager.get_params()
        self.utils_engine = create_engine(
            self.params["conn_string_publish"] + self.params["schema_publish"]
        )
        self.valuation_date = params_dict["VALUATION_DATE"]
        self.valuation_date = dt.strptime(self.valuation_date, "%Y%m%d").date()
        self.previous_valuation_date = self.valuation_date - timedelta(days=46)
        print("La fecha previa es:", self.previous_valuation_date)
        self.param_days = params_dict["NODES"]
        self.param_days = self.param_days.split(",")
        self.param_days = [int(numero) for numero in self.param_days]
        self.curve_name = params_dict["CURVE_NAME"]
        logger.info(
            f"La decha es: {self.valuation_date}, los nodos son: {self.param_days} y el nombre de la curva es: {self.curve_name}"
        )

    def validation(self, node):
        select_query = text(
            f"""
            SELECT curve,days, rate, valuation_date FROM pub_otc_inter_swap_cc_daily\
            WHERE status_info = 1 AND valuation_date BETWEEN :previous_valuation_date AND :value_date AND days = :node\
            AND curve = :curve\
            ORDER BY valuation_date DESC;
            """
        )
        query_params = {
            "node": node,
            "previous_valuation_date": self.previous_valuation_date,
            "curve": self.curve_name,
            "value_date": self.valuation_date,
        }
        request = pd.read_sql(select_query, con=self.utils_engine, params=query_params)
        if request.empty:
            select_query = text(
                f"""
            SELECT curve,days, rate, valuation_date FROM pub_otc_inter_swap_cross_daily\
            WHERE status_info = 1 AND valuation_date BETWEEN :previous_valuation_date AND :value_date AND days = :node\
            AND curve = :curve\
            ORDER BY valuation_date DESC;
            """
            )
            query_params = {
                "node": node,
                "previous_valuation_date": self.previous_valuation_date,
                "curve": self.curve_name,
                "value_date": self.valuation_date,
            }
            request = pd.read_sql(
                select_query, con=self.utils_engine, params=query_params
            )

        data = request[["rate"]][1:].copy()
        valuation_date_value = request.at[0, "rate"]
        valuation_date_value_yesterday = request.at[1, "rate"]
        percentile_10 = np.percentile(data, 10)
        percentile_90 = np.percentile(data, 90)
        is_valid_day = True
        if (
            valuation_date_value <= percentile_10
            or valuation_date_value >= percentile_90
        ):
            logger.info(
                "Por favor revise la curva ya que sobrepasa los valores permitidos"
            )
            is_valid_day = False
        else:
            logger.info("Validacion correcta")

        percentage_change = (
            (valuation_date_value - valuation_date_value_yesterday)
            / valuation_date_value_yesterday
        ) * 100
        logger.info(
            "La variacion porcentual para la curva es: " + f"{percentage_change}" + " %"
        )
        return is_valid_day, valuation_date_value

    def run(self):
        invalid_days = []
        for day in self.param_days:
            validation = self.validation(day)
            if not validation[0]:
                invalid_days.append((day, validation[1]))
        print(invalid_days)
        if len(invalid_days) > 0:
            invalid_nodes_df = pd.DataFrame(invalid_days, columns=["days", "rate"])
            body = f"""
                Cordial saludo.
                
                Para la curva ({self.curve_name}) con fecha de valoracion ({self.valuation_date}) los siguientes nodos no pasaron la validacion:
 
                {invalid_nodes_df.to_string(index=False)}
 
                Megatron.
 
                Enviado por el servicio automatico de notificaciones de Precia PPV S.A. en AWS"""
            subject = "Validacion SwapCC no aprobada"
            send_email(body, self.smtp_secret, subject)


if __name__ == "__main__":
    data = swapvalidator()
    data.run()
