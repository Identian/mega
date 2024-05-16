"""
===============================================================================

Nombre: glue-p-otc-opi-collector-fenics.py

Tipo: Glue Job

Autora:
    - Lorena Julieth Torres Hernández
    Tecnologia - Precia

Ultima modificacion: 10/10/2023

En este script se realiza la recolección del archivo de opciones internacional 
del broker FENICS, tambien se realiza la notificación sobre el estado de la recolección

Parametros del Glue Job (Job parameters o Input arguments):
    "--SFTP_SECRET"= <<Nombre del secreto de SFTP FENICS>>
    "--SMTP_SECRET"= <<Nombre del secreto del servidor de correos>>
    "--VALUATION_DATE"= DATE (viene de la maquina de estados sm-*-otc-fwd-inter-points)
    "--PARAMETER_STORE" = <<Nombre del parametro que contiene el nombre de la lambda que actualiza los estados>>
    "--FENICS_FILENAME" = <<Estructura del nombre del archivo  FENICS>>
    "--BUCKET_NAME" = <<Nombre del bucket donde se alojara el archivo recolectado>>


===============================================================================
"""

import logging
import sys
import base64
import boto3
import json
import paramiko
import gzip
from awsglue.utils import getResolvedOptions
from email.message import EmailMessage
import logging
import smtplib


ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")

#PRECIA_UTILS_EXCEPTIONS 
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


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


#PRECIA_UTILS_LOGGER
def setup_logging():
    """
    formatea todos los logs que invocan la libreria logging
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = logging.StreamHandler(sys.stdout)
    precia_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
    logger.setLevel(logging.INFO)
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
  
 
#PRECIA_UTILS_AWS
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
        cliente_secrets_manager = boto3.client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = base64.b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


def get_params(parameter_list: list) -> dict:
    """
    Obtiene los parametros configurados en 'Job parameters' en el Glue Job

    Args:
        parameter_list (list): Lista de parametros a recuperar

    Returns:
        dict: diccionario de los parametros solicitados
    """
    try:
        logger.info("Obteniendo parametros ...")
        params = getResolvedOptions(sys.argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except (Exception,) as sec_exc:
        error_msg = (
            f"No se encontraron todos los parametros solicitados: {parameter_list}"
        )
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc
    

logger = setup_logging()


try:
    params_key = [
        "SFTP_SECRET",
        "SMTP_SECRET",
        "VALUATION_DATE",
        "PARAMETER_STORE",
        "BUCKET_NAME",
        "FENICS_FILENAME",
        "PATH_OPI_FENICS"
    ]
    params_dict = get_params(params_key)
    secret_sftp = params_dict["SFTP_SECRET"]
    secret_smtp = params_dict["SMTP_SECRET"]  
    valuation_date = params_dict["VALUATION_DATE"]
    parameter_store_name = params_dict["PARAMETER_STORE"]
    bucket_name = params_dict["BUCKET_NAME"]
    fenics_filename = params_dict["FENICS_FILENAME"]
    

    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc


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

    def __init__(self, subject, body, data_file = None) -> None:
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
            connection = smtplib.SMTP(host=secret["server"], port=secret["port"])
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
            logger.info("Se realiza envio de mensaje.")
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


class send_notification_mail:
    def __init__(self, valuation_date):
        self.valuation_date = valuation_date
        

    def send_error_mail(self, conn_msg, error_msg):
        """Se realiza el envió del correo en caso de que la recolección sea fallida

        Args:
            conn_msg (str): Mensaje en caso de que falle la recoleccion
            error_msg (str): error técnico

        Raises:
            PlataformError: error técnico
        """
        
        try:
            logger.info("Se intenta notificar el error en la recoleccion...")
            subject = "Megatron: Fenics, Error en recolección Opciones internacional"
            body = f"Cordial saludo.\n\n\n\n {conn_msg} para la fecha {self.valuation_date}\n\nEl error que se presento fue: {error_msg}\n\n Megatron.\n\n  Enviado por el servicio automatico de notificaciones de Precia PPV S.A. en AWS"
            email = ReportEmail(subject, body)
            key_secret = get_secret(secret_smtp)
            mail_from =  key_secret["mail_from"]
            mail_to = key_secret["mail_to"] 
            data_connection_smtp = {
                "server": key_secret["smtp_server"],
                "port": key_secret["smtp_port"],
                "user": key_secret["smtp_user"],
                "password": key_secret["smtp_password"]}
            
            smtp_connection = email.connect_to_smtp(data_connection_smtp)
            logger.debug(mail_to)
            mail_to = mail_to.split(',')
            for mailto in mail_to:
                message = email.create_mail_base(mail_from, mailto)
                email.send_email(smtp_connection, message)
            logger.info("Se notifica el error en la recoleccion.")
        except (Exception,) as e:
            error_msg_log = 'No se logro enviar el correo para notificar el error en la recolección'
            logger.error(create_log_msg(error_msg_log))
            raise PlataformError(error_msg_log) from e


    def send_successful_mail(self, conn_msg):
        """Se realiza el envió del correo en caso de que la recolección sea exitosa

        Args:
            conn_msg (str): Mensaje en caso de que la recoleccion sea exitosa
        Raises:
            PlataformError: error técnico
        """
        try:
            logger.info("Se intenta notificar el exito en la recoleccion...")
            subject = "Megatron: Fenics, Recolección Opciones internacional"
            body = f"Cordial saludo.\n\n\n\n {conn_msg} para la fecha {self.valuation_date}\n\n\n Megatron.\n\n  Enviado por el servicio automatico de notificaciones de Precia PPV S.A. en AWS"
            email = ReportEmail(subject, body)
            key_secret = get_secret(secret_smtp)
            mail_from =  key_secret["mail_from"]
            mail_to = key_secret["mail_to"] 
            data_connection_smtp = {
                "server": key_secret["smtp_server"],
                "port": key_secret["smtp_port"],
                "user": key_secret["smtp_user"],
                "password": key_secret["smtp_password"]}
            
            smtp_connection = email.connect_to_smtp(data_connection_smtp)
            logger.debug(mail_to)
            mail_to = mail_to.split(',')
            for mailto in mail_to:
                message = email.create_mail_base(mail_from, mailto)
                email.send_email(smtp_connection, message)
            logger.info("Se notifica el exito en la recoleccion")
        except (Exception,) as e:
            error_msg_log = 'No se logro enviar el correo para notificar que la recolección fue exitosa'
            logger.error(create_log_msg(error_msg_log))
            raise PlataformError(error_msg_log) from e
        
        
class FileManager:
    MAX_FILE_SIZE = 5e7
    def __init__(self, path, filename, bucket_name, valuation_date) -> None:
        
        self.path = path
        self.filename = filename
        self.bucket_name = bucket_name
        self.valuation_date = valuation_date
        
        self.client = self.connect_to_sftp()
        self.fenics_opi_file = self.get_data_file_from_sftp(self.path)
        self.upload_s3(self.fenics_opi_file, self.filename)
        

    def connect_to_sftp(self, timeout: int = 10) -> paramiko.SSHClient:
        """
        Se conecta a un SFTP usando las credenciales contenidas en 'secret'
        y la libreria 'paramiko'. Las credenciales 'secret' se almacenan en Secrets Manager

        :returns: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
        """
        raise_msg = "No fue posible conectarse al SFTP destino"
        email_actions = send_notification_mail(self.valuation_date)
        try:
            
            logger.info("Conectandose al SFTP ...")
            logger.info("Validando secreto del SFTP ...")
            
            secret = get_secret(secret_sftp)
            logger.debug(secret)
            sftp_host = secret["sftp_host"]
            sftp_port = secret["sftp_port"]
            sftp_username = secret["sftp_user"]
            sftp_password = secret["sftp_password"]
            
            logger.info("Secreto del SFTP tienen el formato esperado.")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
            client.connect(
                sftp_host,
                port=sftp_port,
                username=sftp_username,
                password=sftp_password,
                timeout=timeout,
            )
            logger.info("Conexion al SFTP exitosa.")
            self.sftp = client.open_sftp()
            conn_msg = 'La conexion hacia el servidor de SFTP de FENICS fue exitosa'
            email_actions.send_successful_mail(conn_msg)
        except KeyError as key_exc:
            error_msg = "El secreto del SFTP no tiene el formato esperado"
            logger.error(create_log_msg(error_msg))
            conn_msg = 'La conexion hacia el servidor de SFTP de FENICS falló'
            email_actions.send_error_mail(conn_msg, key_exc)
            raise PlataformError(raise_msg) from key_exc
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            conn_msg = 'La conexion hacia el servidor de SFTP de FENICS falló'
            email_actions.send_error_mail(conn_msg,unknown_exc)
            raise PlataformError(raise_msg) from unknown_exc


    def get_data_file_from_sftp(self, sftp_path):
        """Descarga el archivo desde el SFTP de Fenics
        
        Parameters:
            sftp_path (str): Ruta del archivo
        Returns:
            bytes: Data del archivo
        """
        try:
            with self.sftp.open(sftp_path, 'rb') as remote_file:
                filesize = self.sftp.stat(sftp_path).st_size
                if filesize > FileManager.MAX_FILE_SIZE:
                    logger.error("El archivo supera el tamaño permitido")
                    raise ValueError("El archivo es demasiado grande, supera el limite permitido")
                fenics_opi_file = remote_file.read()
                try:
                    fenics_opi_file = gzip.decompress(fenics_opi_file)
                except (Exception ,) :
                    logger.info('No se logro descoprimir el archivo')
            logger.info("Se ha descargado el archivo correctamente desde el SFTP de Fenics")
            try:
                logger.info('Se intenta realizar la desconexión del SFTP de Fenics...')
                self.sftp.close()
            except Exception as e:
                logger.error(create_log_msg('No se logro cerrar la conexión de forma correcta'))
            return fenics_opi_file
        except Exception as e:
            logger.error(create_log_msg(f"Ocurrio un error en la descarga desde el SFTP de Fenics del archivo: {str(e)}"))
            
            raise PlataformError("No se pudo descargar el archivo desde el SFTP de Fenics")
        
        
    def upload_s3(self, data, file_name):
        """Se sube los archivos al bucket de validacion

        Args:
            data (_type_): Data recolectada del SFTP de FENICS
            file_name (_type_): nombre del archivo que se va a almacenar en el S3

        Raises:
            PlataformError: Error en tal caso que falle el cargue del archivo
        """
        
        try:
            s3 = boto3.client('s3')
            file_name = file_name.replace(".gz", "")
            s3.put_object(Body=data, Bucket=self.bucket_name, Key=f"Fenics/{file_name}")
            logger.info(f"Se ha cargado el archivo {file_name} en el bucket {self.bucket_name} correctamente")
            
        except Exception as e:
            error_msg = f"Ocurrio un error en la carga del archivo del bucket {self.bucket_name}. ERROR: {e}"
            logger.error(create_log_msg(error_msg))
            
            raise PlataformError(error_msg) from e
  
        
def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = boto3.client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from lbd_exc


def get_parameter_from_ssm(parameter_name):
    """
    Obtiene un parámetro de Amazon Systems Manager (SSM) Parameter Store.

    Parámetros:
        parameter_name (str): El nombre del parámetro que se desea obtener.

    Retorna:
        str: El valor del parámetro almacenado en SSM Parameter Store.

    Excepciones:
        ssm_client.exceptions.ParameterNotFound: Si el parámetro no existe en SSM Parameter Store.
        Exception: Si ocurre un error inesperado al obtener el parámetro desde SSM Parameter Store.
    """
    ssm_client = boto3.client('ssm')
    try:
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        logger.info("Se obtuvo el parametro correctamente")
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        logger.error(f"El parámetro '{parameter_name}' no existe en Parameter Store.")
    except Exception as e:
        logger.error(f"Error al obtener el parámetro '{parameter_name}' desde r Parameter Store: {e}")


def update_report_process(status, description, technical_description):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """
    lambda_name = get_parameter_from_ssm(parameter_store_name)
    report_process = {
          "input_id": "Opciones Internacionales",
          "output_id":"Opciones Internacionales",
          "process": "Derivados OTC",
          "product": "opt_inter",
          "stage": "Recoleccion",
          "status": "",
          "aws_resource": "glue-p-otc-opi-collector-fenics",
          "type": "insumo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")
    
def run():
    try:
        file_date = valuation_date.replace('-', '')
        filename_fenics = fenics_filename.replace('YYYYMMDD', file_date)
        path_opi_sftp = params_dict["PATH_OPI_FENICS"]
        logger.info(path_opi_sftp)
        path_opi_sftp = path_opi_sftp + filename_fenics
        FileManager(path_opi_sftp, filename_fenics, bucket_name, valuation_date)
        update_report_process("Exitoso", "Proceso Finalizado", "")
    except (Exception,) as init_exc:
        error_msg = "Fallo la ejecución del main, por favor revisar:"
        update_report_process("Fallido", error_msg, str(init_exc))
        raise PlataformError() from init_exc      


if __name__ == "__main__":
    run()