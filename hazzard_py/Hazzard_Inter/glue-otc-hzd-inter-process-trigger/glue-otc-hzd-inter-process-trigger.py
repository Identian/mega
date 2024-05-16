"""Módulo que se encarga de gestionar los reportes de los procesos necesarios para Hazzard Internacional,
verificar que esten completos para cada curva y lanzar el proceso para Hazzard"""

from itertools import product
from awsglue.utils import getResolvedOptions
from base64 import b64decode
from boto3 import client as aws_client
import json
import logging
import pandas as pd
import sqlalchemy as sa
from sys import argv, exc_info, stdout


ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)


#-------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
_bootstrap_error_message = 'Fallo el calculo del error bootstrap'
_bootstrap_error_raise_message ="No se pudo calcular el error bootstrap para la curva con los insumos dados"
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
        raise
        
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
        raise

#---------------------------------------------------------------------------------


class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.connection = None
        self.valuation_date = valuation_date
        self.url_db = url_db

    def connect_db(self):
        """Genera la conexión a la base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            logger.info("Se conecto correctamente a la base de datos")
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise

    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def disable_previous_info(self, status_table: str, valuation_date: str, product: str, input_name: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        try:
            update_query = (
                "UPDATE " + status_table + ""
                + " SET last_status= 0"
                + " WHERE product = '" + product + "'"
                + " AND input_name IN (" + input_name + ")"
                + " AND valuation_date IN (" + valuation_date + ")"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg("Falló la actualización del estado de la información"))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))

    def insert_data_db(self, df_info_process: pd.DataFrame, status_table: str):
        """Inserta la informacion de los procesos reportados"""
        try:
            df_info_process.to_sql(status_table, con=self.connection, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg("Falló la inserción de la información en la base de datos"))
            raise PlataformError("No fue posible insertar la información en la base de datos: " + str(e))

    def check_dependencies_hzd(self, cds_name: str, curve_swap: str):
        """Obtiene el numero de los procesos reportados en base de datos"""
        process = f"'{cds_name}', '{curve_swap}'"
        query_dependencies = sa.sql.text(""
            + "SELECT COUNT(input_name) as amount_processes FROM precia_utils_swi_status_cross_dependencies "
            + " WHERE product IN ('Swap Inter', 'CDS')"
            + " AND input_name IN (" + process + ")"
            + " AND valuation_date = :valuation_date "
            + " AND last_status = :last_status "
        "")
        query_params = {
            "last_status": 1,
            "valuation_date": self.valuation_date
        }
        try:
            reported_processes = pd.read_sql(query_dependencies, self.connection, params=query_params)
            number_processes = reported_processes.at[0, 'amount_processes']
            logger.info("Se obtuvo la información de los procesos reportados para cross")
            return number_processes
        except Exception as e:
            logger.error(create_log_msg("Fallo la extracción de la informacion de la base de datos"))
            raise PlataformError("No se pudo traer la info de base de datos: " + str(e))


def generate_df(product_name: str, valuation_date: list, input_name: list):
    """Genera el dataframe con la informacion a insertar de los procesos"""
    processes_and_dates = list(product(input_name, valuation_date))
    df_report_process = pd.DataFrame(processes_and_dates, columns=["input_name", "valuation_date"])
    df_report_process["product"] = product_name
    df_report_process["status_process"] = "successful"
    return df_report_process


def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
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
        raise PlataformError(raise_msg) from lbd_exc


def get_dependency_list(dependency_str: str):
    return [element.strip("' ") for element in dependency_str.split(',')]


def check_and_launch(db_handler: DbHandler, is_dependencie_cds: bool, dependencies: list, curve_swap: str, lambda_name: str, message_reports: str, valuation_date: str):
    """Verifica que las dependencias esten para los Hazzard y lanza la lambda del proceso"""
    if is_dependencie_cds:
        for cds in dependencies:
            logger.info(f"CDS A VALIDAR:{cds}")
            reports = db_handler.check_dependencies_hzd(cds, curve_swap)
            if reports == 2:
                payload_hzd_inter = {
                    "VALUATION_DATE": valuation_date,
                    "HAZZARD": cds
                }
                launch_lambda(lambda_name=lambda_name, payload=payload_hzd_inter)
                logger.info(f"Se lanza el glue para el proceso de metodología de Hazzards Internacional: {curve_swap}")
            else:
                logger.info(f"No se han reportado todas las dependencias para {cds}: {reports}" + message_reports)


def main():
    params_key = [
        "DB_SECRET",
        "INPUT_NAME",
        "LAMBDA_TRIGGER_HZD_INTER",
        "PRODUCT",
        "VALUATION_DATE"
    ]
    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc + schema_precia_utils_otc
    

    product_name = params_glue["PRODUCT"]

    valuation_date = list(params_glue["VALUATION_DATE"].split(","))
    valuation_dates_str = ",".join(["'{}'".format(element) for element in valuation_date])

    input_name = list(params_glue["INPUT_NAME"].split(","))   
    input_names = ",".join(["'{}'".format(element) for element in input_name])

    dependencies_hzd = {
        'SwapCC_USDOIS': ['CO', 'BA', 'CB', 'GS', 'JP', 'MS', 'WEF', 'BR', 'RBOC'],
        'SwapCC_EUROIS': ['BCS', 'BBV', 'BNP', 'CIT', 'DB', 'HBC', 'HS', 'INGB', 'SAN', 'SCB', 'SCP', 'UBS', 'NS'],
        'SwapCC_JPYOIS': ['SUM', 'MTFC']
    }

    dependencies_usdois = dependencies_hzd['SwapCC_USDOIS']
    dependencies_eurois = dependencies_hzd['SwapCC_EUROIS']
    dependencies_jpyois = dependencies_hzd['SwapCC_JPYOIS']

    product_list = ["Swap Inter", "CDS"]
    name_lambda_trigger_hzd = params_glue["LAMBDA_TRIGGER_HZD_INTER"]

    

    is_product = product_name in product_list
    is_cds_usdois = any(element in dependencies_usdois for element in input_name)
    is_cds_eurois = any(element in dependencies_eurois for element in input_name)
    is_cds_jpyois = any(element in dependencies_jpyois for element in input_name)

    df_report_process = generate_df(product_name, valuation_date, input_name)

    # Inserta los procesos reportados
    db_handler_sources = DbHandler(url_db_precia_utils_otc, valuation_date[0])
    db_handler_sources.connect_db()
    db_handler_sources.disable_previous_info("precia_utils_swi_status_cross_dependencies", valuation_dates_str,
                                             product_name, input_names)
    db_handler_sources.insert_data_db(df_report_process, "precia_utils_swi_status_cross_dependencies")

    message_reports = "\n o se ha lanzado el proceso cross anteriormente para esta curva"

    check_and_launch(db_handler_sources, is_cds_usdois, dependencies_usdois, 'SwapCC_USDOIS',
                      name_lambda_trigger_hzd, message_reports, valuation_date[0])

    check_and_launch(db_handler_sources, is_cds_eurois, dependencies_eurois, 'SwapCC_EUROIS',
                      name_lambda_trigger_hzd, message_reports, valuation_date[0])

    check_and_launch(db_handler_sources, is_cds_jpyois, dependencies_jpyois, 'SwapCC_JPYOIS',
                      name_lambda_trigger_hzd, message_reports, valuation_date[0])

    db_handler_sources.disconnect_db()

    if not is_product:
        logger.info("El producto reportado no es dependencia para Hazzard Inter")


if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main()
