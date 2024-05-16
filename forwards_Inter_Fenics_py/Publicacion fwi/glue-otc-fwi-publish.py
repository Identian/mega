import logging
import pandas as pd
import sys
import base64
import boto3
import json
import paramiko
import io
import os
import hashlib
import time
#TODO descomentar en el glue
from awsglue.utils import getResolvedOptions
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from io import BytesIO
import logging
import smtplib


 
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


ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")

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
        "SMTP_SECRET",
        "VALUATION_DATE",
        "INSTRUMENT",
        "PARAMETER_STORE",
        "PATH_FILE_OUT",
        "PATH_FILE_IN",
        "BUCKET_SOURCE",
        "BUCKET_VALIDATOR",
        "SFTP_SECRET",
        "FTP_SECRET",
        "PATH_S3_VALIDATOR",
        "NUM_RETRIES",
        "TIME_RETRY"
    ]
    params_dict = get_params(params_key)
    secret_smtp = params_dict["SMTP_SECRET"]   
    parameter_store_name = params_dict["PARAMETER_STORE"]
    valuation_date = params_dict["VALUATION_DATE"]
    instrument = params_dict["INSTRUMENT"]
    path_file_out = params_dict["PATH_FILE_OUT"]
    path_file_in = params_dict["PATH_FILE_IN"]
    bucket_source = params_dict["BUCKET_SOURCE"]
    bucket_validator = params_dict["BUCKET_VALIDATOR"]
    sfpt_secret = params_dict["SFTP_SECRET"]
    fpt_secret = params_dict["FTP_SECRET"]
    path_s3_validator = params_dict["PATH_S3_VALIDATOR"]
    num_retries = params_dict["NUM_RETRIES"]
    time_retry = params_dict["TIME_RETRY"]
    

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

    def attach_file_to_message(self, message):
        """Adjunta el archivo al mensaje(object EmailMessage) para enviar por el correo
        Parameters:
        -----------
        message: Object EmailMessage, required
            Contiene el mensaje básico del correo

        Returns:
        Object EmailMessage
            Tiene los archivos adjuntos junto a la data básica el mensaje

        Raises:
        -------
        PlataformError
            Si no pudo adjuntar los archivos al mensaje
        """

        logger.info("Comienza adjuntar los archivos...")
        for file_name, file_data in self.data_file.items():
            df_file_data = pd.DataFrame(file_data)
            try:
                buf = BytesIO()
                df_file_data.to_csv(buf, index=False)
                buf.seek(0)
                binary_data = buf.read()
                maintype, _, subtype = (
                    mimetypes.guess_type(file_name)[0] or "application/octet-stream"
                ).partition("/")
                message.add_attachment(
                    binary_data, maintype=maintype, subtype=subtype, filename=file_name
                )
            except (Exception,) as att_exc:
                logger.error(
                    create_log_msg("Fallo en adjuntar los archivos en el mensaje")
                )
                raise PlataformError(
                    "Hubo un error al adjuntar los archivos"
                ) from att_exc

        logger.info("Termino de adjuntar los archivos en el mensaje correctamente")
        return message

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


class FileManager:

    def __init__(self, instrument, valuation_date, path_out, path_in, bucket_name, validator_bucket):
        self.valuation_date = valuation_date
        self.instrument = instrument
        self.path_out = path_out
        self.path_in = path_in
        self.bucket_name = bucket_name
        self.validator_bucket = validator_bucket
        
        
        

    def create_paths(self):

        valuation_date = self.valuation_date.replace("-", "")
        self.path_fwd = f"{self.path_out}Fwd_{self.instrument}_Nodos_{valuation_date}.txt"
        self.path_fwdt2 = f"{self.path_out}FwdT2_{self.instrument}_Nodos_{valuation_date}.txt"
        self.path_fwd2 = f"{self.path_out}Fwd2_{self.instrument}_Nodos_{valuation_date}.csv"
        self.path_fwd_daily = f"{self.path_out}Fwd_{self.instrument}_Diaria_{valuation_date}.txt"
        self.path_in_fwd = f"{self.path_in}Fwd_{self.instrument}_Nodos_{valuation_date}.txt"
     
        
    def download_file_s3(self, bucket_name, key):
        s3 = boto3.client("s3")
        try:
            logger.info('Se intenta descargar el archivo en el S3...')
            
            s3_object = s3.get_object(Bucket=bucket_name, Key=key)
            file_origin = s3_object['Body'].read()
            logger.info('Se descarga el archivo del S3')
            return file_origin
        except Exception as e:
            error_msg = "No se logro  descargar los archivos en el S3"
            logger.error(create_log_msg(error_msg))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)
            raise PlataformError(error_msg) from e
               

    def move_to_sftp(self, data, name_file, valuation_date):
        try:
            secret = get_secret(fpt_secret)
            timeout = int(2)
            logger.info("Conectandose al FTP ...")
            logger.info("Validando secreto del FTP ...")
            sftp_host = secret["sftp_host"]
            sftp_port = secret["sftp_port"]
            sftp_username = secret["sftp_user"]
            sftp_password = secret["sftp_password"]
            logger.info("Secreto del FTP tienen el formato esperado.")
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
            sftp_client = client.open_sftp()
            ftp_secret = get_secret(fpt_secret)
            route_path = ftp_secret["route_fwd"]
            logger.info("Conexion al SFTP exitosa.")    
        except KeyError as key_exc:
            error_msg = "El secreto del SFTP no tiene el formato esperado"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(raise_msg) from key_exc
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(raise_msg) from unknown_exc   
        name_file = name_file.split('/')[-1]
        new_dir = valuation_date
        try:
            route_path = os.path.join(route_path, new_dir)
            sftp_client.mkdir(route_path)
        except OSError:
            pass
        route_path = os.path.join(route_path, name_file)
        try:
            if sftp_client==None:
                logger.info('No se establece conexion con ftp')
            else:
                
                logger.info(route_path)
                logger.debug('Se intenta crear el archivo en el FTP...')
                with sftp_client.open(route_path, 'w') as f:
                    logger.debug('Se intenta escribir el archivo en el FTP...')
                    f.write(data)
                logger.info(f"Se ha publicado el archivo {name_file} correctamente")
        except Exception as e:
            error_msg = f"Ocurrio un error en la publicacion del archivo: {name_file}"
            logger.error(error_msg)
            logger.error(create_log_msg(error_msg))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)   
            raise PlataformError(error_msg) from e


    def load_files_to_sftp(self, route_sftp, file_name,file_content):
        
        sftp_connect = self.sftp_client.open_sftp()
        logger.info("Comenzando a generar el archivo %s en el sftp", file_name)
        error_message = f"No se pudo generar el archivo en el SFTP: {file_name}"
        try:
            with sftp_connect.open(route_sftp + "/" + file_name, "w") as f:
                try:
                    if file_name.lower().endswith(".csv"):
                        f.write(file_content.to_csv(index=True, sep=" ", line_terminator='\r\n', header=True))
                    else:
                        f.write(file_content.decode)
                except (Exception,):
                    error_msg = "Fallo la escritura del archivo:" + str(file_name)
                    logger.error(create_log_msg(error_msg))
                    raise

        except (Exception,) as e:
            logger.error(create_log_msg(error_message))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)
            raise PlataformError(error_msg) from e
        finally:
            sftp_connect.close()
            
    def generate_sha256_from_s3_file(self, data_file, file_name):
        """Genera el sha256 del archivo original que viene de s3"""
        try:
            logger.debug(file_name)
            hash_value = hashlib.sha256(data_file).hexdigest()
            hash_file_name = file_name.split('/')[-1] + '.sha256'
            logger.info(f"Se ha generado el sha correctamente del archivo {file_name}")
            return hash_value, hash_file_name
        except (Exception, ) as e:
            error_msg = f"Ocurrio un error en la generacion del sha del archivo {file_name}"
            logger.error(create_log_msg(error_msg))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)
            raise PlataformError(error_msg) from e
            

    def download_ftp(self, name_file, valuation_date):
        """Descarga el archivo y sha de FTP de clientes"""
        try:
            secret = get_secret(fpt_secret)
            timeout = int(2)
            logger.info("Conectandose al FTP ...")
            logger.info("Validando secreto del FTP ...")
            sftp_host = secret["sftp_host"]
            sftp_port = secret["sftp_port"]
            sftp_username = secret["sftp_user"]
            sftp_password = secret["sftp_password"]
            logger.info("Secreto del FTP tienen el formato esperado.")
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
            sftp_client = client.open_sftp()
            ftp_secret = get_secret(fpt_secret)
            route_path = ftp_secret["route_fwd"]
            
            logger.info("Conexion al SFTP exitosa.")    
        except KeyError as key_exc:
            error_msg = "El secreto del SFTP no tiene el formato esperado"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(raise_msg) from key_exc
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(raise_msg) from unknown_exc   
        name_file = name_file.split('/')[-1]
        new_dir = valuation_date
        try:
            route_path = os.path.join(route_path, new_dir)
            sftp_client.mkdir(route_path)
        except OSError:
            pass
        route_path = os.path.join(route_path, name_file)
        try:
            with sftp_client.open(route_path, 'rb') as remote_file:
                file_data = remote_file.read()
            logger.info(f"Se ha descargado el archivo {name_file} correctamente del FTP")
            return file_data
        except Exception as e:
            error_msg = f"Ocurrio un error en la descarga del FTP del archivo: {name_file}"
            logger.error(create_log_msg(error_msg))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)  
            raise PlataformError(error_msg) from e
            

    def upload_s3(self, bucket_name, path_s3_validator, data, file_name):
        """Se sube los archivos al bucket de validacion"""
        try:
            s3 = boto3.client('s3')
            s3.put_object(Body=data, Bucket=bucket_name, Key=f"{path_s3_validator}/{file_name}")
            logger.info(f"Se ha cargado el archivo {file_name} en el bucket {bucket_name} correctamente")
        except Exception as e:
            error_msg = f"Ocurrio un error en la carga del archivo del bucket {bucket_name}. ERROR: {e}"
            logger.error(create_log_msg(error_msg))
            email_actions = send_mail_error(self.valuation_date, self.instrument)
            email_actions.send_error_mail(error_msg)    
            raise PlataformError(error_msg) from e


    def process_s3_files(self):

        self.create_paths()
        s3_data = [self.path_fwd, self.path_fwdt2, self.path_fwd2, self.path_fwd_daily, self.path_in_fwd]
        for file_path in s3_data:
            logger.debug(file_path)
            data_file_origin = self.download_file_s3(self.bucket_name, file_path)
            file_name = file_path.split("/")[-1]
            hash_value, hash_file_name = self.generate_sha256_from_s3_file(data_file_origin, file_name)            
            self.move_to_sftp( data_file_origin, file_name, self.valuation_date)
            self.move_to_sftp(hash_value, hash_file_name, self.valuation_date)
            data_original = self.download_ftp(file_name, self.valuation_date)
            data_hash = self.download_ftp( hash_file_name, self.valuation_date)
            self.upload_s3(self.validator_bucket, path_s3_validator, data_original, file_name)
            self.upload_s3(self.validator_bucket, path_s3_validator, data_hash, hash_file_name)

 
        
class send_mail_error:
    def __init__(self, valuation_date, instrument):
        self.valuation_date = valuation_date
        self.instrument = instrument

    def send_error_mail(self, error_msg):
        """
        Se realiza el envió del correo 
        """
        try:
            
            subject = f"Megatron: Se presento un error al momento de publicar forward internacional {self.instrument}"
            body = f"Se presento un error al momento de realizar la publicacion para el instrumento {self.instrument} para la fecha {self.valuation_date}\n\n {error_msg}\n\n\n\n Megatron"
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
            message = email.create_mail_base(mail_from, mail_to)
            email.send_email(smtp_connection, message)
        except (Exception,) as e:
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


def update_report_process(status, description, technical_description, instrument):
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
          "input_id": "Forward_" + str(instrument),
          "output_id":[instrument],
          "process": "Derivados OTC",
          "product": "fwd_inter",
          "stage": "Publicacion",
          "status": "",
          "aws_resource": "glue-p-otc-fwi-publish",
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
    instruments = instrument.split(", ")
    try:
        for instrument_fwd in instruments:
            filemanager_actions = FileManager(instrument_fwd, valuation_date, path_file_out, path_file_in, bucket_source, bucket_validator)
            logger.info('Inicia metodo principal')
            filemanager_actions.process_s3_files()    
            update_report_process("Exitoso", "Proceso Finalizado", "", instrument_fwd)
            logger.info("Se finaliza la ejecucion del main")
    except Exception as e:
        logger.error(f"No se logro ejecutar el main, error: {e}")

if __name__ == "__main__":
    for attemp in range(int(num_retries)):
       try: 
        run()
        break
       except Exception as e:
           logger.info(f'Se presento un error en el reintento {int(attemp) +1}')
           if attemp < int(num_retries)-1:
               logger.info(f'Se va a volver a intentar en {int(time_retry)} segundos')
               time.sleep(int(time_retry))
    else:
        logger.error(create_log_msg('Fallaron todos los reintentos'))
        raise PlataformError() 
           