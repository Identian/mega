import logging
import sys
import base64
import boto3
import json
import pandas as pd
from io import StringIO
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
import logging
import sqlalchemy as sa

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

class MissingData(Exception):
    def __init__(
        self,
        error_message="Faltaron algunos tenores, ver el log para mas detalles",
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
######

try:
    params_key = [
        "DB_SECRET",
        "PARAMETER_STORE",
        "BUCKET_NAME",
        "FILE_PATH_S3",
        "LBD_VALIDATOR",
        "S3_SCHEMA_BUCKET",
        "OPI_CURRENCY_LIST",
        "LBD_OPI_PROCESS"
    ]
    params_dict = get_params(params_key)
    parameter_store_name = params_dict["PARAMETER_STORE"]
    bucket_name = params_dict["BUCKET_NAME"]
    file_path_s3 = params_dict["FILE_PATH_S3"]
    lambda_schema_validator = params_dict["LBD_VALIDATOR"]
    s3_schema_bucket = params_dict["S3_SCHEMA_BUCKET"]
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url_sources = db_url_dict["conn_string_sources"]
    schema_utils = db_url_dict["schema_precia"]
    schema_sources = db_url_dict["schema_sources"]
    db_url_utils = db_url_sources + schema_utils
    db_url_src = db_url_sources + schema_sources
    lambda_trigger_process = params_dict["LBD_OPI_PROCESS"]
    #Get_name_file - valuation_date
    opi_file_s3 = file_path_s3.split("/")[1]
    valuation_date = opi_file_s3.split("_")[2]
    valuation_date = (datetime.strptime(valuation_date, "%Y%m%d")).date()
    valuation_date = datetime.strftime(valuation_date, "%Y-%m-%d")
    logger.info('Se obtuvieron los parametros')
    
 
    
    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc

######

class actions_db():
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
            logger.info("Creando el engine de conexion...")
            sql_engine = sa.create_engine(
                self.db_url_src , connect_args={"connect_timeout": 2}
            )
            logger.info("Engine de conexion creado con exito")
            logger.info("Creando conexion a BD...")
            db_connection = sql_engine.connect()
            logger.info("Conexion a BD creada con exito")
            return db_connection
        except (Exception,) as conn_exc:
            raise_msg = "Fallo la creacion de la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc
        
    

    def insert_data(self, info_df):
        """
        Se realiza la insercion de la información consultada
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                logger.debug(info_df)
                info_df.to_sql('src_otc_options_inter', con=db_connection, if_exists="append", index=False)
                logger.info(actions_db.CONN_CLOSE_ATTEMPT_MSG)
                db_connection.close()
                logger.info("Conexion a BD cerrada con exito")
                logger.info("Insercion de dataframe en BD exitosa")
            except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
    
        
    def get_id_precia(self):

        select_query = ("SELECT precia_alias FROM precia_utils_suppliers_api_dictionary"+
                        " WHERE precia_utils_suppliers_api_dictionary.id_byproduct = 'opt_inter'"+
                        " AND precia_utils_suppliers_api_dictionary.alias_type = 'id';"
        )
        db_connection = self.create_connection()
        fwd_params_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return fwd_params_df 
    

    def disable_previous_info(self, info_df: pd.DataFrame):
        """
        Funcion encargada de deshabilitar la información anterior en la
        tabla, solo si se encuentra en la necesidad de volver a cargar la información
        """        
        error_msg = "No se pudo realizar la deshabilitacion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            conn = db_connection.connect()
            
            try:
                logger.debug(info_df)
                info_df = info_df.reset_index()
                
                id_precia_value_list = (info_df['id_precia'].tolist())
                id_precia_list = []
                for id_supplier in id_precia_value_list:
                    if id_supplier not in id_precia_list:
                        id_precia_list.append(id_supplier)
                id_precia_list = tuple(id_precia_list)
                info_df.set_index("id_precia", inplace=True)
                valuation_date_str = info_df.at[id_precia_value_list[0], "Valuation_date"]
                status_info = 0
                update_query = ("UPDATE src_otc_options_inter SET status_info= {} WHERE id_precia in {} and valuation_date = '{}'".format(status_info,id_precia_list, valuation_date_str))
                conn.execute(update_query)
                
            except sa.exc.SQLAlchemyError as sql_exc:
                logger.error(create_log_msg(error_msg))
                raise_msg = (
                    "La base de datos no acepto los datos. validar tipos de datos"
                )
                raise PlataformError(raise_msg) from sql_exc
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
       


class extract():
    def __init__(self, bucket_name) -> None:
        
        self.bucket_name = bucket_name
        
        
    def download_file_s3(self):
        s3 = boto3.client("s3")
        try:
            logger.info('Se intenta descargar el archivo en el S3...')
            logger.info(file_path_s3)
            s3_object = s3.get_object(Bucket=self.bucket_name, Key=file_path_s3)
            file_origin = s3_object['Body'].read()
            logger.info('Se descarga el archivo del S3')
            return file_origin
        except Exception as e:
            error_msg = "No se logro  descargar los archivos en el S3"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from e




class transform:
    def __init__(self, file) -> None:
        self.file = file

        
    def modify_opi_df(self, file, file_path):
        try:
            logger.info("Se inicia con la adecuación de la estructura del dataframe para iniciar su procesamiento...")
            file = file.decode('utf-8')
            opi_file = pd.read_csv(StringIO(file), skiprows=[0], skipfooter=1, engine='python')
            opi_file_2 = opi_file.copy()
            opi_file_2.rename(columns={'CcyPair': 'Currency', 'PriceType': 'Strategy', 'Time': 'Val_date'}, inplace=True)
            systems = file_path.split("/")[0]
            if systems == "CargaManual":
                opi_file_2["Systems"] = "Valoracion"  
            else:
                opi_file_2["Systems"] = systems
            opi_file_2 = opi_file_2[["Systems","Currency", "Tenor", "Strategy", "Mid", "Bid", "Ask", "Val_date"]]
            logger.info("Se finaliza la adecuación de la estructura del dataframe.")
            logger.debug(opi_file_2)
            logger.debug(opi_file)
            return opi_file_2, opi_file
        except (Exception,) as e:
            error_msg = "Fallo la adaptacion del dataframe"
            raise PlataformError(error_msg) from e        

    
    def filter_strategies(self, opi_file):
        try:
            logger.info("Se intenta filtrar por estrategias necesarias...")
            strategies_list = ["S10","RR10","S25","RR25","ATMF"]
            opi_file_filter = opi_file[opi_file['Strategy'].isin(strategies_list)]
            logger.debug(opi_file_filter)
            logger.info("Se filtrar por estrategias necesarias.")
            return opi_file_filter
        except (Exception,) as e:
            error_msg = "No se logro filtrar por las estrategias necesarias"
            raise PlataformError(error_msg) from e  


    def rename_strategies(self, opi_filer):
        try:
            logger.info("Se intenta renombrar las estrategias...")
            opi_filer["Strategy"] = opi_filer["Strategy"].replace({"S10": "10BF", "RR10": "10RR","S25": "25BF", "RR25": "25RR", "ATMF":"ATM"}, regex=True)
            logger.debug(opi_filer)
            logger.info("Se renombran las estrategias.")
            return opi_filer
        except (Exception,) as e:
            error_msg = "No se logro renombrar las estrategias."
            raise PlataformError(error_msg) from e  
 
    
    def validate_id_precia (self, opi_df, df_precia_id):
        try:
            logger.info("Se intenta validar si la informacion esta completa...")
            opi_df['id_precia'] = "Opc_"+opi_df["Strategy"]+"_"+opi_df["Currency"]+"_"+opi_df["Tenor"]
            opi_df['id_precia'] = opi_df['id_precia'].str.replace("1W","SW", regex=True)
            opi_df['Tenor'] = opi_df['Tenor'].str.replace("1W","SW", regex=True)
            precia_id_list = df_precia_id['precia_alias'].unique()
            opi_df_id_precia = opi_df['id_precia'].unique()
            if set(precia_id_list).issubset(set(opi_df_id_precia)):
                logger.info('Los datos requeridos para el porceso se encuentran completos')
            else:
                missing_data = set(precia_id_list)- set(opi_df_id_precia)
                logger.info('Faltan los tenores...')
                logger.error(create_log_msg(f'Falto la siguiente informacion: {str(missing_data)}'))
                raise MissingData()
            opi_df = opi_df[opi_df["id_precia"].isin(precia_id_list)]
            opi_df = opi_df[["Systems","id_precia", "Currency", "Strategy", "Tenor", "Mid", "Bid", "Ask", "Val_date"]]
            logger.debug(opi_df)
            logger.info("Se crea la columna id_precia.")
            return opi_df
        except (Exception,) as e:
            error_msg = "No se logro validar si la informacion esta completa"
            raise PlataformError(error_msg) from e     
   
        
    def columns_format(self, opi_file):
        try:
            logger.info("Se intenta cambiar el fotmato de la columna Val_date...")
            opi_file['Val_date'] = (opi_file['Val_date'].str.strip(" "))
            opi_file_split = opi_file["Val_date"].str.split(" ", n=1, expand=True)
            opi_file["Valuation_date"] = opi_file_split[0]
            opi_file["Valuation_date"] = opi_file["Valuation_date"].apply(lambda x: datetime.strptime(x, "%d-%b-%Y").strftime("%Y-%m-%d"))
            opi_file["status_info"] = 1
            opi_file = opi_file.fillna(0)
            opi_file = opi_file[["Systems","id_precia", "Currency", "Strategy", "Tenor", "Mid", "Bid", "Ask","Valuation_date", "status_info"]]
            logger.debug(opi_file)
            
            logger.info("Se cambia el formato de la columna valuation_date.")
            return opi_file
        except (Exception,) as e:
            error_msg = "No se modificar el formato de las columnas"
            raise PlataformError(error_msg) from e    
        
    def get_list_instruments(self, opi_file):
        try: 
            logger.info("Intentanto obtener listado de monedas para opciones internacional")
            opi_currency_list = opi_file["Currency"].to_list()
            opi_currency_list = set(opi_currency_list)
            logger.info(opi_currency_list)

            return opi_currency_list
        except (Exception,) as e:
            error_msg = "No se logra obtener el listado de los monedas para opciones internacional"
            raise PlataformError(error_msg) from e        
        
        
class load():
    def __init__(self, file) -> None:
        self.file = file
        
        
        
    def load_data_db(self, opi_file):
        try:
            logger.info("Se intenta cargr la infomracion en base de datos...")
            acctiondb = actions_db(db_url_src)
            acctiondb.disable_previous_info(opi_file)
            acctiondb.insert_data(opi_file)
            logger.info("Se logra cargar la información en base de datos.")
        except (Exception,) as e:
            error_msg = "No se logro filtrar por las estrategias necesarias"
            raise PlataformError(error_msg) from e     
        
        
    def load_data_bucket(self, bucket_name, data, file_path_s3, s3_schema_bucket):
        """Se sube los archivos para validar su estructura"""
        try:
            logger.debug(data)
            s3 = boto3.client('s3')
            file_name = file_path_s3.split("/")[1]
            csv_buffer = BytesIO()
            data.to_csv(csv_buffer, index=False)
            data = csv_buffer.getvalue()
            s3.put_object(Body=data, Bucket=bucket_name, Key=f"Megatron/{file_name}")
            logger.info(f"Se ha cargado el archivo {file_name} en el bucket {bucket_name} correctamente")
            payload  = {
                "s3CsvPath": f"Megatron/{file_name}",
                "s3SchemaPath": "options/international/opi_fenics_schema.csvs",
                "s3SchemaDescriptionPath": "options/international/opi_fenics_schema_description.txt",
                "s3SchemaBucket": s3_schema_bucket,
                "s3CsvBucket": bucket_name,
            }
            return payload
        except Exception as e:
            error_msg = f"Ocurrio un error en la carga del archivo del bucket {bucket_name}. ERROR: {e}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from e


def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        logger.info(payload)
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
          "output_id":["Opciones Internacionales"],
          "process": "Derivados OTC",
          "product": "opt_inter",
          "stage": "Almacenamiento",
          "status": "",
          "aws_resource": "glue-p-otc-opi-etl-fenics",
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
        logger.info(db_url_utils)
        actiondb = actions_db(db_url_utils)
        df_precia_id = actiondb.get_id_precia()
        file = extract(bucket_name)
        opi_file_s3 = file.download_file_s3()
        opi_file = transform(opi_file_s3)
        opi_file_modify, opi_file_modify_2 = opi_file.modify_opi_df(opi_file_s3,file_path_s3)
        load(opi_file_s3)
        payload_bucket = load.load_data_bucket(bucket_name,bucket_name, opi_file_modify_2,file_path_s3, s3_schema_bucket)
        logger.info('Se lanza lambda para validar la estructura del archivo')
        launch_lambda(lambda_schema_validator, payload_bucket)
        opi_file_filter = opi_file.filter_strategies(opi_file_modify)
        opi_df_rename = opi_file.rename_strategies(opi_file_filter)
        opi_file_id = opi_file.validate_id_precia(opi_df_rename, df_precia_id)
        opi_file_to_db = opi_file.columns_format(opi_file_id)

        load.load_data_db(opi_file_to_db,opi_file_to_db)
        opi_currency_list = opi_file.get_list_instruments(opi_file_to_db)
        for currency in opi_currency_list:
            payload = {
                "opi_currency": currency,
                "valuation_date": valuation_date
            }
            
            launch_lambda(lambda_trigger_process, payload)
        update_report_process("Exitoso", "Proceso Finalizado", "")
    except (Exception,) as init_exc:
        error_msg = "Fallo la ejecución del main, por favor revisar:"
        update_report_process("Fallido", error_msg, str(init_exc))
        raise PlataformError() from init_exc      


if __name__ == "__main__":
    run()