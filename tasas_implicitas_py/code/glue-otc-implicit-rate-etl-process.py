"""
Glue que se encarga de la metodología de tasas implícitas 
"""
# Nativas
from base64 import b64decode
from dateutil import relativedelta as rd
import datetime as dt
from decimal import Decimal
from email.message import EmailMessage
from io import BytesIO
import json
import logging
import mimetypes
import smtplib
from sys import argv, exc_info, stdout

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client

# De terceros
import paramiko
import pandas as pd
import numpy as np
import sqlalchemy as sa

ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)
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
    file_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
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
        

#-------------------------------------------------------------------------------------------------------------------
# OBTENER SECRETOS Y PARÁMETROS
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
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
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

#--------------------------------------------------------------------------------
# EJECUTAR LAMBDA

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
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise
    
#---------------------------------------------------------------------------------
# REPORTE DE ESTADOS
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

    lambda_name = get_parameter_store(PARAMETER_STORE)
    implict_rate_param = get_params(["IMPLICIT_RATE"])
    implict_rate = implict_rate_param["IMPLICIT_RATE"]
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": implict_rate,
          "output_id":[implict_rate],
          "process": "Derivados OTC",
          "product": "Implicit_Rate",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-otc-implicit-rate-etl-process",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")

#---------------------------------------------------------------------------------
# ENVIO DE CORREOS
class ReportEmail:
    """Clase que representa los correos que reportan las variaciones del día"""
    def __init__(self, subject, body) -> None:
        self.subject = subject
        self.body = body
        

    def connect_to_smtp(self, secret):
        """Conecta al servicio SMTP de precia con las credenciales que vienen en secret"""
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
        """Crea el objeto EmailMessage que contiene todos los datos del email a enviar"""
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
        """Envia el objeto EmailMessage construido a los destinatarios establecidos"""
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


#---------------------------------------------------------------------------------
# METODOLOGÍA TASAS IMPLICITAS

def implicit_rate(fwd_points_pair_1, spot_pair_1, factor_1 ,discount_curve, fwd_nodes_1, pair_curr_1):
    """
    Construye las tasas implicitas del par o par cruzado dado los puntos forward.
    
    Params
    ----------
    fwd_points_pair_1 (pd.dataframe): Dataframe con la curva diaria de puntos forward para el par principal
                               days:  Dias de la curva.
                               mid_fwd: Mid puntos forward
                               bid_fwd: Bid puntos forward
                               ask_fwd: Ask puntos forward
    spot_pair_1 (float): Tasa de cambio spot del par principal   
    factor_1 (str): Factor de los puntos forward del par principal                                                          
    discount_curve (numpy.ndarray) : Curva de descuento.
    fwd_nodes_1 (numpy.ndarray): Dias de los nodos de la curva diaria de puntos forward para el par principal
    pair_curr_1 (str): Par principal
    
    Returns
    ------------
    implicit_rates_nodes (pd.dataframe): Curva de tasas implicitas nodos
                               days:  Dias de la curva.
                               mid: Tasa implicita mid
                               bid: Tasa implicita bid
                               ask: Tasa implicita ask
    implicit_rates_daily (pd.dataframe): Curva de tasas implicitas diaria
                               days:  Dias de la curva.
                               mid: Tasa implicita mid
                               bid: Tasa implicita bid
                               ask: Tasa implicita ask    
    """
    try:
        base_curr_1 = pair_curr_1[0:3]
        fwd_points_pair_1.iloc[:,[1,2,3]] = fwd_points_pair_1.iloc[:,[1,2,3]].round(8)
        fwd_nodes_days = fwd_nodes_1
        fwd_days = fwd_points_pair_1.iloc[:,0]
        if base_curr_1 =="USD":
            implied_rate_curve = ((1+fwd_points_pair_1.iloc[:,[1,2,3]]/(spot_pair_1*factor_1)).mul((1+discount_curve[fwd_days-1]*fwd_days/360),axis=0)-1).mul(360/fwd_days,axis=0)
        else:
            implied_rate_curve = ((1+fwd_points_pair_1.iloc[:,[1,2,3]]/(spot_pair_1*factor_1)).rdiv((1+discount_curve[fwd_days-1]*fwd_days/360),axis=0)-1).mul(360/fwd_days,axis=0)
    
        implied_rate_curve = pd.concat([fwd_days,implied_rate_curve],axis=1)
        implied_rate_curve.columns = ["days","mid","bid","ask"]
        implied_rate_curve_nodes = implied_rate_curve.iloc[fwd_nodes_days-1,:]
        return implied_rate_curve_nodes,implied_rate_curve
    except(Exception,):
        logger.error(create_log_msg(f'Se genero un error en el calculo de las tasas implicitas del par {pair_curr_1}'))
        raise PlataformError(f"Hubo un error en el calculo de las tasas implicitas del par {pair_curr_1}")

#---------------------------------------------------------------------------------
# EXTRACCIÓN DE INFORMACIÓN
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
            raise PlataformError("Hubo un error en la conexión a base de datos: " + str(e))


    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None
    
    
    def get_data_factor_rate(self, id_precia: str):
        """Trae los factores de las tasas implicitas y el nombre de la swap que depende"""
        query_factors = sa.sql.text("""
            SELECT factor_rate as factor, swap_name
            FROM precia_utils_factor_implicit_rate
            WHERE id_precia = :id_precia
        """)
        query_params = {
            "id_precia": id_precia
        }
        error_message = "No se pudo traer el factor de la tasa"
        try:
            df_factors = pd.read_sql(query_factors, self.connection, params=query_params)
            logger.debug(df_factors)
            logger.info(f"Se obtuvo el factor de la tasa: {id_precia}")
            return df_factors
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+id_precia))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer el factor de la tasa: {id_precia}: "+ str(e))
            

    def get_data_rate(self, id_precia: str):
        """Trae el valor de la tasa"""
        id_precia = id_precia.replace('2','')
        query_exchange_rate = sa.sql.text("""
            SELECT value_rates FROM pub_exchange_rate_parity
            WHERE id_precia = :id_precia
            AND valuation_date = :valuation_date
            AND status_info = 1
        """)
        query_params = {
            "id_precia": id_precia,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información de la tasa"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información de la tasa: {id_precia}")
            if df_exchange_rate.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de la tasa: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+id_precia))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información de la tasa {id_precia}: "+ str(e))
        

    def get_data_swapcc_inter_daily(self, curve: str):
        """Trae la información de los Swap Inter diaria"""
        query_swap_daily = sa.sql.text("""
            SELECT days, rate FROM pub_otc_inter_swap_cc_daily
            WHERE curve = :curve
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "curve": curve,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información del Swap"
        try:
            df_swapcc_daily = pd.read_sql(query_swap_daily, self.connection, params=query_params)
            logger.debug(df_swapcc_daily)
            logger.info(f"Se obtuvo la información del Swap: {curve}")
            if df_swapcc_daily.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos del Swap: {self.valuation_date}")
            return df_swapcc_daily
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información de la curva {curve}: "+ str(e))
            
            
    def get_data_fwd_inter_daily(self, curve: str):
        """Obtiene la información de Forward Inter Diaria"""
        query_fwd_inter_daily = sa.sql.text("""
            SELECT days_fwd as days, mid_fwd, bid_fwd, ask_fwd 
            FROM pub_otc_fwd_inter_daily
            WHERE instrument_fwd = :curve
            AND valuation_date = :valuation_date
            ORDER BY days_fwd ASC
        """)
        query_params = {
            "curve": curve,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del Forward Inter Diaria "
        try:
            df_fwd_inter_daily = pd.read_sql(query_fwd_inter_daily, self.connection, params=query_params)
            logger.debug(df_fwd_inter_daily)
            logger.info(f"Se obtuvo la información de Forward Inter Diaria de la curva {curve} exitosamente")
            if df_fwd_inter_daily.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Forward Inter Diaria: {self.valuation_date}")
            return df_fwd_inter_daily
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve+str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError (f"No se pudo traer la info del Forward nodos {curve}" + str(e))
            
    
    def get_data_fwd_inter_nodes(self, curve: str):
        """Obtiene la información de Forward Inter Nodos"""
        query_fwd_inter_nodes = sa.sql.text("""
            SELECT days_fwd AS days, tenor_fwd AS tenor 
            FROM pub_otc_forwards_inter_points_nodes
            WHERE instrument_fwd = :curve
            AND valuation_date = :valuation_date
            ORDER BY days_fwd ASC
        """)
        query_params = {
            "curve": curve,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del Forward Inter Nodos "
        try:
            df_fwd_inter_nodes = pd.read_sql(query_fwd_inter_nodes, self.connection, params=query_params)
            logger.debug(df_fwd_inter_nodes)
            logger.info(f"Se obtuvo la información de Forward Inter Nodos de la curva {curve} exitosamente")
            if df_fwd_inter_nodes.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Forward Inter Nodos: {self.valuation_date}")
            return df_fwd_inter_nodes
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve+str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError (f"No se pudo traer la info del Forward nodos {curve}" + str(e))
        
    
    def disable_previous_info(self, pub_table: str, valuation_date: str, id_precia: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ pub_table + ""
                + " SET status_info= 0"
                + " WHERE id_precia = '"+ id_precia + "'"
                + " AND valuation_date = '"+ valuation_date + "'"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))
        

    def disable_previous_info_process(self, status_table: str, valuation_date: str, product: str, input_name: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ status_table + ""
                + " SET last_status= 0"
                + " WHERE product = '"+ product + "'"
                + " AND input_name = '"+ input_name + "'"
                + " AND valuation_date ='"+ valuation_date +"'"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))
    

    def insert_data_db(self, df_insert: pd.DataFrame, pub_table: str):
        """Inserta la informacion calculada durante el proceso"""
        error_message = "Falló la inserción de la información en la base de datos"
        try:
            df_insert.to_sql(pub_table, con=self.connection, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible insertar la información en la base de datos: "+ str(e))
        
    
#------------------------------------------------------------------------------------------------------------
# CARGA DE INFORMACIÓN
class Loader:
    """Representa la generación de archivos tasas implicitas"""

    def __init__(self, data_connection_sftp: dict, route_sftp: str) -> None:
        self.data_connection = data_connection_sftp
        self.route_sftp = route_sftp


    def connect_sftp(self, timeout: int = 40) -> paramiko.SSHClient:
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
            self.sftp_connect = self.ssh_client.open_sftp()
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise Exception(raise_msg) from unknown_exc
    
    
    def disconnect_sftp(self):
        """Cierra la conexión al SFTP"""
        if self.sftp_connect is not None:
            self.sftp_connect.close()
            self.sftp_connect = None
        
        
    def load_files_to_sftp(self, df_file: pd.DataFrame, file_name: str, columns_file: list):
        """Genera y carga los archivos en el SFTP de tasas implicitas"""
        logger.info("Comenzando a generar el archivo %s en el sftp", file_name)
        error_message = "No se pudo generar el archivo en el SFTP"
        try:
            with self.sftp_connect.open(self.route_sftp + file_name, "w") as f:
                try:
                    f.write(df_file.to_csv(index=False, sep=" ", line_terminator='\r\n', header=False, columns=columns_file))
                except Exception as e:
                    logger.error(create_log_msg(f"Fallo la escritura del archivo: {file_name}"))
                    raise PlataformError("No fue posible la escritura del archivo: "+ str(e))

        except Exception as e:
            logger.error(create_log_msg(error_message+": "+file_name))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible generar el archivo {file_name} en el SFTP: "+ str(e))
            
#-----------------------------------------------------------------------------------------
class ETL():
    """Representa la orquestación de la ETL"""
    def __init__(self, url_precia_utils: str, url_publish_otc: str,  url_sirius: str) -> None:
        self.url_publish_otc = url_publish_otc
        self.url_precia_utils = url_precia_utils
        self.url_sirius = url_sirius
        valuation_date_param = get_params(["VALUATION_DATE"])
        self.valuation_date = valuation_date_param["VALUATION_DATE"]
        rate_implict_param = get_params(["IMPLICIT_RATE"])
        self.implicit_rate = rate_implict_param["IMPLICIT_RATE"]

    
    def extract_data(self):
        """Orquesta la extracción de la informacion de la clase DbHandler"""
        logger.info("Comienza la extraccion de informacion de base de datos...")
        try:
            self.db_handler_utils = DbHandler(self.url_precia_utils, self.valuation_date)
            self.db_handler_utils.connect_db()
            self.df_factor = self.db_handler_utils.get_data_factor_rate(self.implicit_rate)
            self.db_handler_utils.disconnect_db()
            swap_name = str(self.df_factor.loc[0, 'swap_name'])
            self.factor = int(self.df_factor.loc[0, 'factor'])
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
        
        try:
            self.db_handler_rates = DbHandler(self.url_sirius, self.valuation_date)
            self.db_handler_rates.connect_db()
            self.df_exchange_rate = self.db_handler_rates.get_data_rate(self.implicit_rate)
            self.exchange_rate = float(self.df_exchange_rate.loc[0, 'value_rates'])
            self.db_handler_rates.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
        
        try:
            self.db_handler_pub = DbHandler(self.url_publish_otc, self.valuation_date)
            self.db_handler_pub.connect_db()
            self.df_swapcc_inter = self.db_handler_pub.get_data_swapcc_inter_daily(swap_name)
            self.df_fwd_inter_daily = self.db_handler_pub.get_data_fwd_inter_daily(self.implicit_rate)
            self.df_fwd_inter_nodes = self.db_handler_pub.get_data_fwd_inter_nodes(self.implicit_rate)
            self.db_handler_pub.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)

        
    def transform_data(self):
        """Orquesta la construcción de las Tasas Implícitas, implementación de la metodología"""
        try:
            logger.info("Inicia la construcción de las tasas implicitas (Implementación de la metodología)...")
            self.nodes_rates, self.daily_rates = implicit_rate(self.df_fwd_inter_daily,  self.exchange_rate, self.factor, 
                                                               self.df_swapcc_inter["rate"].values, self.df_fwd_inter_nodes["days"].values, self.implicit_rate)
            
            # Columnas adicionales para inserción en base de datos
            self.nodes_rates["valuation_date"] = self.valuation_date
            self.daily_rates["valuation_date"] = self.valuation_date
            self.nodes_rates["id_precia"] = self.implicit_rate
            self.daily_rates["id_precia"] = self.implicit_rate

            # Redondear los df nodos y diarios a 10 decimales
            self.nodes_rates = self.nodes_rates.round(decimals=10)
            self.daily_rates = self.daily_rates.round(decimals=10)
            
            # Para mantener los decimales exactos de los archivos
            self.nodes_rates['mid'] = self.nodes_rates['mid'].apply(decimal_convert)
            self.nodes_rates['bid'] = self.nodes_rates['bid'].apply(decimal_convert)
            self.nodes_rates['ask'] = self.nodes_rates['ask'].apply(decimal_convert)
            
            self.daily_rates['mid'] = self.daily_rates['mid'].apply(decimal_convert)
            self.daily_rates['bid'] = self.daily_rates['bid'].apply(decimal_convert)
            self.daily_rates['ask'] = self.daily_rates['ask'].apply(decimal_convert)

        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
        
        
    def load_info(self, data_connection_sftp, route_implicit_rate):
        """Orquesta la generación de archivos y la inserción a la base de datos de Swap Inter - CCS"""
        report_cross = {
            'product': ["Rate Implicit"],
            'input_name':[self.implicit_rate],
            'status_process':["successful"],
            'valuation_date':[self.valuation_date]
        }
        
        logger.info(f"Comienza la generación de archivos y la inserción a bd para la tasa implicita: {self.implicit_rate}")
        sufix_nodos_name = "_Nodos_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        sufix_diaria_name = "_Diaria_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        loader = Loader(data_connection_sftp, route_implicit_rate)
        file_name_diaria = "Tasas_" + self.implicit_rate + sufix_diaria_name
        file_name_nodos = "Tasas_" + self.implicit_rate + sufix_nodos_name
        # Generación de los archivos
        try:
            loader.connect_sftp()
            loader.load_files_to_sftp(self.daily_rates, file_name_diaria, columns_file=['days', 'mid', 'bid', 'ask'])
            loader.load_files_to_sftp(self.nodes_rates, file_name_nodos, columns_file=['days', 'mid', 'bid', 'ask'])
            loader.disconnect_sftp()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        finally:
            loader.disconnect_sftp()

        # Inserción en base de datos
        try:
            self.db_loader_pub = DbHandler(self.url_publish_otc, self.valuation_date)
            self.db_loader_pub.connect_db()
            self.db_loader_pub.disable_previous_info("pub_otc_implicit_rate_daily", self.valuation_date, self.implicit_rate)
            self.db_loader_pub.insert_data_db(self.daily_rates, "pub_otc_implicit_rate_daily")
            self.db_loader_pub.disable_previous_info("pub_otc_implicit_rate_nodes", self.valuation_date, self.implicit_rate)
            self.db_loader_pub.insert_data_db(self.nodes_rates, "pub_otc_implicit_rate_nodes")
            self.db_loader_pub.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        logger.info("Finaliza la creación de los archivos y la inserción en la base de datos exitosamente")
        # Reporte del proceso
        df_report_cross = pd.DataFrame(report_cross)
        self.db_loader_utils = DbHandler(self.url_precia_utils, self.valuation_date)
        self.db_loader_utils.connect_db()
        self.db_loader_utils.disable_previous_info_process("precia_utils_swi_status_cross_dependencies", self.valuation_date, "Rate Implicit", self.implicit_rate)
        self.db_loader_utils.insert_data_db(df_report_cross, "precia_utils_swi_status_cross_dependencies")
        self.db_loader_utils.disconnect_db()
        update_report_process("Exitoso", "Proceso Finalizado", "")

    
    def send_error_email(self, error_msg: str):
        """Envía los correos cuando ocurra un error en el proceso"""
        params_key = ["SMTP_SECRET"]
        params_glue = get_params(params_key)
        secret_smtp = params_glue["SMTP_SECRET"]
        key_secret_smtp = get_secret(secret_smtp)
        data_connection_smtp = {
            "server": key_secret_smtp["smtp_server"],
            "port": key_secret_smtp["smtp_port"],
            "user": key_secret_smtp["smtp_user"],
            "password": key_secret_smtp["smtp_password"],
        }
        mail_from = key_secret_smtp["mail_from"]
        mail_to = key_secret_smtp["mail_to"]

        subject = f"Megatron: ERROR al procesar la Tasa Implicita: {self.implicit_rate}"
        body = f"""
Cordial Saludo.

Durante el procesamiento de la tasa implicita: {self.implicit_rate} se presento el siguiente error en glue.

ERROR: 
{error_msg}

Puede encontrar el log para mas detalles de las siguientes maneras:

    - Ir al servicio AWS Glue desde la consola AWS, dar clic en 'Jobs'(ubicado en el menu izquierdo), dar clic en el job informado en este correo, dar clic en la pestaña 'Runs' (ubicado en la parte superior), y buscar la ejecucion asociada a este mensaje por instrumento, fecha de valoracion y error comunicado, para ver el log busque el apartado 'Cloudwatch logs' y de clic en el link 'Output logs'.

    - Ir al servicio de CloudWatch en la consola AWS, dar clic en 'Grupos de registro' (ubicado en el menu izquierdo), y filtrar por '/aws-glue/python-jobs/output' e identificar la secuencia de registro por la hora de envio de este correo electronico.
Megatron.

Enviado por el servicio automático de notificaciones de Precia PPV S.A. en AWS
        """
        try:
            email = ReportEmail(subject, body)
            smtp_connection = email.connect_to_smtp(data_connection_smtp)
            message = email.create_mail_base(mail_from, mail_to)
            email.send_email(smtp_connection, message)
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
            
            
def decimal_convert(num):
    if pd.notnull(num):
        dec_num = Decimal(str(num))
        return format(dec_num, 'f')
    else:
        return num
        

def main():
    params_key = [
        "DB_SECRET",
        "SFTP_SECRET",
        "SMTP_SECRET",
        "VALUATION_DATE"
    ]

    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_publish_otc = key_secret_db["conn_string_aurora_publish"]
    schema_publish_otc = key_secret_db["schema_aurora_publish"]
    url_db_publish_aurora = url_publish_otc+schema_publish_otc

    url_publish_rates = key_secret_db["conn_string_sirius"]
    schema_publish_rates = key_secret_db["schema_sirius_publish"]
    url_db_publish_sirius = url_publish_rates+schema_publish_rates

    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc+schema_precia_utils_otc

    secret_sftp = params_glue["SFTP_SECRET"]
    key_secret_sftp = get_secret(secret_sftp)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_implicit_rate = key_secret_sftp["route_implicit_rate"]

    etl =  ETL(url_db_precia_utils_otc, url_db_publish_aurora, url_db_publish_sirius)
    etl.extract_data()
    etl.transform_data()
    etl.load_info(data_connection_sftp, route_implicit_rate)
    update_report_process("Exitoso", "Proceso Finalizado", "")



if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main() 