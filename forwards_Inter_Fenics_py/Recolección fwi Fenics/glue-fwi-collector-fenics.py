"""
Módulo para la recolección del archivo Forwards del vendor Fenics
- Recolecta el archivo del SFTP de Fenics 
- Guarda el archivo en el Bucket de S3 
- Envía correo del estado de la recolección
- Envía reporte de estado del proceso
"""

from awsglue.utils import getResolvedOptions
from base64 import b64decode
from boto3 import client as aws_client
from datetime import datetime
from email.message import EmailMessage
from json import loads as json_loads, dumps as json_dumps
import gzip
import logging
import smtplib
from sys import argv, exc_info, stdout

import paramiko

MAX_FILE_SIZE = 5e7
ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PARAMETER_STORE = "ps-otc-lambda-reports"

#-------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
def setup_logging(log_level):
    """
    Configura el sistema de registro de logs.
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        file_handler = logging.StreamHandler(stdout)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"))
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato adecuado al mensaje log_msg, incluyendo información sobre excepciones.
    """
    exception_type, exception_value, exception_traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )

#-------------------------------------------------------------------------------------------------------------------
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
        raise Exception(error_msg) from sec_exc
        
        
def get_parameter_store(parameter_name):
    """
    Obtiene el valor del parameter store
    
    Parameters:
        parameter_name (str): Nombre del parámetro
    
    Returns:
        str: valor del parametro obtenido
    """
    logger.info("Intentando leer el parametro: "+parameter_name)
    ssm_client = aws_client('ssm')
    response = ssm_client.get_parameter(
        Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " +
                str(response['Parameter']['Value']))
    return response['Parameter']['Value']
    
    
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
        raise Exception(error_msg) from sec_exc
        
    
def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Parameters:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = aws_client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json_dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise Exception(raise_msg) from lbd_exc
        
    
def update_report_process(status, description, technical_description, filename):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        filename (str): Nombre del archivo a colocar en el reporte de estado
        
    Returns:
        None
    """
    lambda_name = get_parameter_store(PARAMETER_STORE)
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": filename,
          "output_id":["Forwards"],
          "process": "Derivados OTC",
          "product": "fwd_inter",
          "stage": "Recolección",
          "status": "",
          "aws_resource": "glue-p-fwi-collector-fenics",
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


def upload_s3(bucket_name, path_s3, data):
    """
    Sube el archivo al bucket de S3
    Parameters:
        bucket_name (str): Nombre del bucket
        path_s3 (str): Ruta en el S3
        data (bytes): Archivo a guardar en el S3 
    """
    s3 = aws_client('s3')
    path_s3 = path_s3.replace(".gz", "")
    try:
        s3.put_object(Body=data, Bucket=bucket_name, Key=path_s3)
        logger.info(f"Se ha cargado el archivo {path_s3} en el bucket {bucket_name} correctamente")
    except Exception as e:
        logger.error(f"Ocurrio un error en la carga del archivo del bucket {bucket_name}. ERROR: {e}")
        raise Exception("Hubo un error cargando el archivo en el S3") from e
        
        
class ReportEmail:
    """
    Clase que representa los correos que reportan el estado de la coleccion
    """

    def connect_to_smtp(self, secret):
        """Conecta al servicio SMTP de precia con las credenciales que vienen en secret
        Parameters:
        secret: dict, required
            Contiene las credenciales de conexión al servicio SMTP

        Returns:
        Object SMTP
            Contiene la conexión al servicio SMTP
        """
        error_msg = "No fue posible conectarse al relay de correos de Precia"
        try:
            logger.info("Conectandose al SMTP ...")
            connection = smtplib.SMTP(host=secret["server"], port=secret["port"])
            connection.starttls()
            connection.login(secret["user"], secret["password"])
            logger.info("Conexión exitosa al SMTP")
            return connection
        except (Exception,) as url_exc:
            logger.error(create_log_msg(error_msg))
            raise Exception(error_msg) from url_exc


    def create_mail_base(self, mail_from, mails_to, subject, message):
        """Crea el objeto EmailMessage que contiene todos los datos del email a enviar
        Parameters:
        mail_from: str, required
            Dirección de correo que envía el mensaje
        mails_to: str, required
            Direcciones de correo destinatario

        Returns:
        Object EmailMessage
            Contiene el mensaje base (objeto) del correo
        """
        error_msg = "No se crear el correo para el SMTP"
        body = f"""
Cordial Saludo.

{message}

Puede encontrar el log para mas detalles de las siguientes maneras:

    - Ir al servicio AWS Glue desde la consola AWS, dar clic en 'Jobs'(ubicado en el menu izquierdo), dar clic en el job 'glue-p-fwi-collector-fenics', dar clic en la pestaña 'Runs' (ubicado en la parte superior), y buscar la ejecucion asociada a este mensaje por instrumento, fecha de valoracion y error comunicado, para ver el log busque el apartado 'Cloudwatch logs' y de clic en el link 'Output logs'.

    - Ir al servicio de CloudWatch en la consola AWS, dar clic en 'Grupos de registro' (ubicado en el menu izquierdo), y filtrar por '/aws-glue/python-jobs/output' e identificar la secuencia de registro por la hora de envio de este correo electronico.
Megatron.

Enviado por el servicio automático de notificaciones de Precia PPV S.A. en AWS
        """
        try:
            logger.info('Creando objeto "EmailMessage()" con los datos básicos...')
            message = EmailMessage()
            message["Subject"] = subject
            message["From"] = mail_from
            message["To"] = mails_to
            message.set_content(body)
            logger.info("Mensaje creado correctamente")
            return message
        except (Exception,) as mail_exp:
            logger.error(create_log_msg(error_msg))
            raise Exception(error_msg) from mail_exp


    def send_email(self, smtp_connection, message):
        """Envia el objeto EmailMessage construido a los destinatarios establecidos
        Parameters:
        smtp_connection: Object SMTP, required
            Contiene la conexión con el servicio SMTP
        message: Object EmailMessage
            Mensaje para enviar por correo
        """
        logger.info("Enviando Mensaje...")
        try:
            smtp_connection.send_message(message)
        except (Exception,) as conn_exc:
            logger.error(create_log_msg("No se pudo enviar el mensaje"))
            raise Exception("No se pudo enviar el mensaje") from conn_exc
        finally:
            try:
                smtp_connection.quit()
                logger.info("Conexión cerrada: SMTP")
            except (Exception,) as quit_exc:
                logger.error(create_log_msg("No se pudo cerra la conexión"))
                raise Exception("Hubo un error cerrando la conexión SMTP") from quit_exc


class SFTPFileReader:
    """
    Representa el lector del archivo del SFTP
    Descarga la info del archivo
    """
    
    def __init__(self, data_connection):
        self.data_connection = data_connection
        
        
    def connect_to_sftp(self, timeout: int = 2) -> paramiko.SSHClient:
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
            raise Exception(raise_msg) from unknown_exc
        
        
    def download_file_sftp(self, sftp_path):
        """Lee el archivo del SFTP
        
        Parameters:
            sftp_path (str): Ruta del archivo
        Returns:
            bytes: Data del archivo
        """
        try:
            self.sftp.stat(sftp_path)
        except FileNotFoundError as e:
            logger.error(f"El archivo no existe en el SFTP {str(e)}")
            raise FileNotFoundError("No existe el archivo en el SFTP") from e
        try:
            with self.sftp.open(sftp_path, 'rb') as remote_file:
                filesize = self.sftp.stat(sftp_path).st_size
                if filesize > MAX_FILE_SIZE:
                    logger.error("El archivo supera el tamaño permitido")
                    raise ValueError("El archivo es demasiado grande")
                file_data = remote_file.read()
                file_data = gzip.decompress(file_data)
            logger.info("Se ha descargado el archivo correctamente del SFTP")
            return file_data
        except Exception as e:
            logger.error(create_log_msg(f"Ocurrio un error en la descarga del SFTP del archivo: {str(e)}"))
            raise Exception("No se pudo descargar el archivo del SFTP") from e
            
            
    def disconnect_sftp(self):
        """Cierra la conexión y la sesión hacia el SFTP"""
        if self.sftp is not None:
            self.sftp.close()
            self.ssh_client.close()
            logger.info("Se ha cerrado la conexion al SFTP")
        else:
            logger.info("No hay conexion al servidor SFTP")
        
        
def main():
    parameters = [
        "FILE_NAME_FWD",
        "S3_BUCKET",
        "S3_FILE_PATH",
        "SMTP_SECRET",
        "SFTP_SECRET",
        "VALUATION_DATE"
    ]
    params_glue = get_params(parameters)
    name_file_fwd = params_glue["FILE_NAME_FWD"]
    smtp_secret = params_glue["SMTP_SECRET"]
    key_secret_smtp = get_secret(smtp_secret)
    data_connection_smtp = {
        "server": key_secret_smtp["smtp_server"],
        "port": key_secret_smtp["smtp_port"],
        "user": key_secret_smtp["smtp_user"],
        "password": key_secret_smtp["smtp_password"],
    }
    valuation_date = params_glue["VALUATION_DATE"]
    
    mail_from = key_secret_smtp["mail_from"]
    mail_to = key_secret_smtp["mail_to"]
    subject_error = "Error en la colección del archivo Forwards de Fenics"
    body_error = "La recolección del archivo de Forwards del SFTP de Fenics ha fallado: "
    subject_succesful = "Recolección exitosa de archivo Forwards SFTP Fenics"
    body_succesful = "La recolección del archivo de Forwards del SFTP de Fenics ha sido exitosa: "
    email = ReportEmail()

    sftp_secret = params_glue["SFTP_SECRET"]
    key_secret_sftp = get_secret(sftp_secret)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_fwd = key_secret_sftp["route_forward"]
    
    data_connection_sftp = {
        "host": "ftp.fenicsmd.com",
        "port": "22",
        "username": "precia2",
        "password": "2Pr3c142",
    }
    file_name = name_file_fwd.replace('YYYYMMDD', valuation_date)

    route_fwd = route_fwd+file_name
    email = ReportEmail()
    try:
        sftp_reader = SFTPFileReader(data_connection_sftp)
        sftp_reader.connect_to_sftp()
        file_data = sftp_reader.download_file_sftp(route_fwd)
        sftp_reader.disconnect_sftp()
    except Exception as e:
        error_message = "No se pudo obtener el archivo del SFTP de Fenics"
        smtp_connection = email.connect_to_smtp(data_connection_smtp)
        body_error = body_error + str(e)
        message = email.create_mail_base(mail_from, mail_to, subject_error, body_error)
        email.send_email(smtp_connection, message)
        update_report_process("Fallido", error_message, str(e), file_name)
        raise Exception(error_message) from e
    finally:
        sftp_reader.disconnect_sftp()
        
    s3_bucket_name = params_glue["S3_BUCKET"]
    s3_key = params_glue["S3_FILE_PATH"]
    s3_path = s3_key+file_name

    try: 
        upload_s3(s3_bucket_name, s3_path, file_data)
    except Exception as e:
        error_message = "Hubo un error almacenando el archivo en el S3"
        smtp_connection = email.connect_to_smtp(data_connection_smtp)
        body_error = body_error + str(e)
        message = email.create_mail_base(mail_from, "mfernandez@precia.co", subject_error, body_error)
        email.send_email(smtp_connection, message)
        update_report_process("Fallido", error_message, str(e), file_name)
        raise Exception(error_message) from e
    
    smtp_connection = email.connect_to_smtp(data_connection_smtp)
    message = email.create_mail_base(mail_from, "mfernandez@precia.co", subject_succesful, body_succesful)
    email.send_email(smtp_connection, message)
    update_report_process("Exitoso", "Proceso Finalizado", "", file_name)


if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main()