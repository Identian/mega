"""
Módulo para la recolección las tasas de Bloomberg
- Recolecta los archivos de tasas de Bloomberg
- Inserta los datos en la base de datos
- Actualiza el estado del proceso en la tabla de control
"""
from awsglue.utils import getResolvedOptions
from base64 import b64decode
from boto3 import client as aws_client
import datetime
from json import loads as json_loads, dumps as json_dumps
from sys import argv, exc_info, stdout
import boto3
import io
import json
import logging
import pandas as pd
import re
import sqlalchemy as sa
ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")
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
    except Exception as sec_exc:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}")
        logger.error(create_log_msg(error_msg))
        raise Exception(error_msg) from sec_exc
#-------------------------------------------------------------------------------------------------------------------        
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
#--------------------------------------------------------------------------------------------------------------------
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


def call_lambda(payload, lambda_name):
    # Create an AWS Lambda client
    client = boto3.client('lambda')
    # Invoke the Lambda function
    response = client.invoke(
        FunctionName = lambda_name,
        InvocationType = 'RequestResponse',
        Payload = json_dumps(payload)
    )
    # Print the response from the Lambda function
    logger.info(response['Payload'].read().decode())
    
#-------------------------------------------------------------------------------------------------------------------
class DBConnectorUtils:
    """
    Representa el lector de la base de datos
    Lee los datos de la tabla
    """
    CONN_CLOSE_ATTEMPT_MSG = "Intentando cerrar la conexion a BD"
    def __init__(self, db_url_src : str) -> None:
        """Gestiona la conexion y las transacciones a base de datos
        Args:
            db_url_src  (str): url necesario para crear la conexion a base de datos
        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url_src  = db_url_src 
        except (Exception,) as init_exc:
            error_msg = "Fallo la creacion del objeto DBManager"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from init_exc
        
    def create_connection(self) -> sa.engine.Connection:
        """Crea la conexion a base de datos
        Raises:
            PlataformError: Cuando falla la creacion de la conexion a BD
        Returns:
            sa.engine.Connection: Conexion a BD
        """
        try:
            logger.info("Creando el motor de conexion...")
            sql_engine = sa.create_engine(
                self.db_url_src , connect_args={"connect_timeout": 2}
            )
            logger.info("Motor de conexion creado con exito")
            logger.info("Creando conexion a BD...")
            db_connection = sql_engine.connect()
            logger.info("Conexion a BD creada con exito")
            return db_connection
        except (Exception,) as conn_exc:
            raise_msg = "Fallo la creacion de la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc
        
    def execute_query(self, query):
        """
        Se realiza la insercion de la información consultada
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                result = db_connection.execute(query)
                logger.info(self.CONN_CLOSE_ATTEMPT_MSG)
                db_connection.close()
                logger.info("Conexion a BD cerrada con exito")
                logger.info("Insercion de dataframe en BD exitosa")
                return result
            except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
    
#-------------------------------------------------------------------------------------------------------------------
#PRECIA_UTILS_EXCEPTIONS 
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""
    
#-------------------------------------------------------------------------------------------------------------------
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

def get_country_name(file_name):
    """
    Obtiene el nombre del pais a partir del nombre del archivo
    """
    pattern = r"bloomberg_cds_Cds_(.*?)_\d{4}-\d{2}-\d{2}\.csv"
    
    match = re.search(pattern, file_name)
    if match:
        return match.group(1)
    else:
        return None
    
#-------------------------------------------------------------------------------------------------------------------
def maturity_cds(cds_info, fecha_dt):
    """
    Genera la fecha de vencimiento de un cds dado el tenor, la fecha de valoracion y el conjunto de paramatros del ciclo

    Params:
        cds_info (pd.dataframe): Dataframe con las caracteristicas del ciclo del cds
            tenor: tenor del cds
            month_ini: mes de inicio del primer ciclo
            month_end: mes de finalizacion del primer ciclo
            maturity_month_1: mes vencimiento primer ciclo
            maturity_month_2: mes vencimiento segundo ciclo
        fecha_dt (datetime.date): fecha de valoracion desde la cual se calcula la fecha
        
    Return:
        mat_date (datetime.date): Fecha de vencimiento del cds

    """
    if cds_info["tenor"] == "6M":
        if fecha_dt>=datetime.date(fecha_dt.year,cds_info["month_ini"],20) and fecha_dt<datetime.date(fecha_dt.year,cds_info["month_end"],20):
            month_mat=cds_info["maturity_month_2"]
        else:
            month_mat=cds_info["maturity_month_1"]
        if fecha_dt<datetime.date(fecha_dt.year,cds_info["month_end"],20):
            year_mat=fecha_dt.year
        else:
            year_mat=fecha_dt.year+1
    else:
        tenor_years=int(''.join([char for char in cds_info["tenor"] if char.isdigit()]))
        if fecha_dt>=datetime.date(fecha_dt.year,cds_info["month_ini"],20) and fecha_dt<datetime.date(fecha_dt.year,cds_info["month_end"],20):
            month_mat=cds_info["maturity_month_1"]
        else:
            month_mat=cds_info["maturity_month_2"]
        if fecha_dt<datetime.date(fecha_dt.year,cds_info["month_ini"],20):
            year_mat=fecha_dt.year+tenor_years-1
        else:
            year_mat=fecha_dt.year+tenor_years
    mat_date=datetime.date(year_mat,month_mat,20)
    return mat_date


#-------------------------------------------------------------------------------------------------------------------
class DataFrameUtils:
    
    dictionary_data_frame = None
    
    action_dictionary_db = None
    action_parameters_db = None
    
    def __init__(self, db_url_utils_dictionary : str, db_url_utils_parameters : str) -> None:
        self.action_dictionary_db = DBConnectorUtils(db_url_utils_dictionary)
        self.action_parameters_db = DBConnectorUtils(db_url_utils_parameters)
        
    def get_data_frame(self, file_name, bucket_name):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket = bucket_name, Key = file_name)
        file_content = response['Body'].read().decode('utf-8')
        data_frame = pd.read_csv(io.StringIO(file_content))
        country_name = get_country_name(file_name)
        return {"country_name" : country_name, "data_frame" : data_frame}    
    
    def build_prototype_data_frame(self, precia_alias, country_name, valuation_date):
        # Create a dictionary with mock values for each column
        data = {
            'counterparty': precia_alias.apply(lambda x: x.split("_")[0]),
            'id_precia': precia_alias,
            'tenor': precia_alias.apply(lambda x: x.split("_")[1]),
            'institution': [country_name] * len(precia_alias),
            'valuation_date': [valuation_date] * len(precia_alias),
            'origin_date': [valuation_date] * len(precia_alias),
            'number_tenor': precia_alias.apply(lambda x: x.split("_")[1].replace('Y', '')).astype(int)
        }
        df_prototype = pd.DataFrame(data)
        df_prototype = df_prototype.sort_values(by='number_tenor', ascending=True)
        df_prototype = df_prototype.drop(columns=['number_tenor'])
        df_prototype = df_prototype.reset_index(drop=True)
        return df_prototype
    
    def build_dictionary_data_frame(self, country_name):        
        country_code = country_name[:2].upper()        
        query_dictionary = ("SELECT precia_alias FROM precia_utils_cds_suppliers_dictionary "+
                            "WHERE name_input = '" + country_code + "' " +
                            "AND precia_alias LIKE '" + country_code + "%%' "+
                            "AND status_info = 1 " +
                            "ORDER BY supplier_alias ASC;")        
        return pd.DataFrame(self.action_dictionary_db.execute_query(query_dictionary))
    
    def build_parameters_data_frame(self):
        query_parameters = ("SELECT counterparty, month_ini, month_end, maturity_month_1, maturity_month_2 " +
                            "FROM precia_utils_cds_maturity_parameters")        
        return pd.DataFrame(self.action_parameters_db.execute_query(query_parameters))
    
    def build_cds_data_frame(self, data_frame_from_file, prototype_df):
        df_cds = pd.concat([prototype_df, data_frame_from_file["pxLast"]], axis = 1)
        df_cds = pd.DataFrame(df_cds)
        return df_cds
    
    def merge_cds_df_parameters_df(self, cds_df, parameters_data_frame, valuation_date):
        valuation_date = datetime.datetime.strptime(valuation_date, "%Y-%m-%d").date()
        new_df = cds_df.merge(parameters_data_frame).apply(lambda row: maturity_cds(row, valuation_date), axis = 1)
        return new_df
    
    def build_query_from_data_frame(self, cds_df):
        # Convert DataFrame to list of tuples
        tuple_list = list(cds_df.to_records(index = False))        
        query_builder = "INSERT INTO prc_otc_cds (counterparty, id_precia, tenor, institution, valuation_date, origin_date, mid_price, maturity_date, days) VALUES "                     
        for element in tuple_list:
            query_builder += "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(element[0], element[1], element[2], element[3], element[4], element[5], element[6], element[7], element[8])        
        return "".join(query_builder)[:-1] + ";"
    
    def get_ditionary_data_frame(self):
        return self.dictionary_data_frame
    
#-------------------------------------------------------------------------------------------------------------------
def main():
    parameters = [
        "BUCKET_NAME",
        "FILE_NAME",
        "VALUATION_DATE",
        "DB_SECRET",
        "ETL_PROCESS_LAMBDA"
    ]
    
    params_glue = get_params(parameters)
    bucket_name = params_glue["BUCKET_NAME"]
    
    file_name = params_glue["FILE_NAME"]
    
    valuation_date = params_glue["VALUATION_DATE"]   
    
    db_secret = params_glue["DB_SECRET"]
    db_url_dict = get_secret(db_secret)
    db_url_sources = db_url_dict["conn_string_aurora_sources"]
    db_url_process = db_url_dict["conn_string_aurora_process"]
    schema_utils_dictionary = db_url_dict["schema_utils"]
    schema_utils_parameters = db_url_dict["schema_utils"]
    schema_utils_prc_otc_cds = db_url_dict["schema_process"]
    db_url_utils_dictionary = db_url_sources + schema_utils_dictionary
    db_url_utils_parameters = db_url_sources + schema_utils_parameters
    db_url_prc_otc_cds = db_url_process + schema_utils_prc_otc_cds
    lambda_etl_process = params_glue["ETL_PROCESS_LAMBDA"]
    body_error = "La recolección del archivo de CDS del SFTP Fenics ha fallado: "
    
    try:
        lambda_report = get_parameter_store(PARAMETER_STORE)     
        
        data_frame_utils = DataFrameUtils(db_url_utils_dictionary, db_url_utils_parameters)   
        data_frame_from_file_structure = data_frame_utils.get_data_frame(file_name, bucket_name)
        country_name = data_frame_from_file_structure["country_name"]
        data_frame_from_file = data_frame_from_file_structure["data_frame"]
        
        action_prc_otc_cds_db = DBConnectorUtils(db_url_prc_otc_cds)
        
        dictionary_data_frame = data_frame_utils.build_dictionary_data_frame(country_name)
        precia_alias = dictionary_data_frame["precia_alias"]

        prototype_df = data_frame_utils.build_prototype_data_frame(precia_alias, country_name, valuation_date)
        
        # Mix the dataframes cds_df and data_frame_from_file
        cds_df = data_frame_utils.build_cds_data_frame(data_frame_from_file, prototype_df)
        
        parameters_data_frame = data_frame_utils.build_parameters_data_frame()
        
        cds_df["maturity_days"] = data_frame_utils.merge_cds_df_parameters_df(cds_df, parameters_data_frame, valuation_date)
        # Convertir las columnas de fecha al tipo datetime
        cds_df['maturity_days'] = pd.to_datetime(cds_df['maturity_days'])
        cds_df['valuation_date'] = pd.to_datetime(cds_df['valuation_date'])
        
        cds_df["days"] = (cds_df["maturity_days"] - cds_df["valuation_date"]).dt.days
    
        cds_df.sort_values(by='days', inplace=True)
        cds_df['pxLast']=cds_df['pxLast']/10000
        logger.info("CDS a insertar: ", cds_df)
        
        
        query_insert_new_records = data_frame_utils.build_query_from_data_frame(cds_df)
    
        
        query_invalidate_previous_records = ("UPDATE prc_otc_cds " +
            "SET status_info= 0 " +
            "WHERE counterparty = '"+cds_df.at[1, 'counterparty']+"' " +
            "AND status_info = 1 " +   
            "AND valuation_date = '" + valuation_date + "' " +
            ";")
        action_prc_otc_cds_db.execute_query(query_invalidate_previous_records)
        action_prc_otc_cds_db.execute_query(query_insert_new_records)
        
        logger.info("Inserción de datos satisfactoria.")
        
        payload = {
            "instrument": [country_name[:2].upper()],
            "valuation_date": valuation_date
        }               
        
        call_lambda(payload, lambda_etl_process)   
        
    except Exception as e:
        error_message = "No se pudo actualizar la información en la base de datos."
        body_error = body_error + str(e)
        payload = {
            "input_id": "Cds_Bloomberg",
            "output_id": [
                "Cds"
            ],
            "process": "Derivados OTC",
            "product": "CDS",
            "stage": "Recoleccion",
            "status": "Fallido",
            "aws_resource": "glue-p-otc-cds-etl-bloomberg",
            "type": "insumo",
            "description": "Ha ocurrido un error al intentar insertar las tasas de Bloomberg en la base de datos.",
            "technical_description": body_error,
            "valuation_date": valuation_date
        } 
        call_lambda(payload, lambda_report)
        raise Exception(error_message) from e    
        
    
#-------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    
    main()