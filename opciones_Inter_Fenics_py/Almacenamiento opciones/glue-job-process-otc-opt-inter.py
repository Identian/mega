"""
=============================================================

Nombre: glue-job-dev-process-otc-opt-inter.py
Tipo: Glue Job

Autor:
    - Ruben Antonio Parra Medrano
Tecnología - Precia

Ultima modificación: 01/11/2023

Script que contiene el main que instancia diferentes
clases que permiten el procesamiento de archivos insumo de
Refinitiv desde un bucket S3 para publicar los datos
resultantes en las bases de datos en source y publish,
los datos de publish son también publicados como archivos
en un FTP conforme a los parámetros:


=============================================================
"""
import boto3
from datetime import datetime, timedelta
import json
import pandas as pd
import sqlalchemy as sa
import sqlalchemy as sql
import logging

from opt_inter_process.VolatilitySurface import VolatilitySurface
from opt_inter_utils.FileManager import FileManager
from opt_inter_utils.ForwardNode import ForwardNode
#from opt_inter_utils.PublishInBD import PublishInBD
from opt_inter_utils.ReportEmail import ReportEmail
from opt_inter_utils.StatusReport import StatusReport
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError, DependencyError
from precia_utils.precia_logger import setup_logging, create_log_msg
#todo quitar

from precia_utils.precia_db import connect_db_by_secret

logger = setup_logging(logging.INFO)


date_format = "%Y-%m-%d"
class PublishInBD:

    """
    Agrupa las funciones necesarias para publicar la informacion generada
    por el proceso de opciones internacionales en el esquema de la base
    datos de publicacion (pub_otc)
    """

    db_connection = None

    def __init__(self, *dataframes) -> None:
        """
        Crea una instancia de PublishInBD (objeto). PublishInBD Agrupa las funciones necesarias
        para publicar la informacion de superficies generada por el proceso de opciones
        internacionales en el esquema de base datos de publicacion (pub_otc)

        Args:
            *dataframes (tuple, list, dict o 4 dataframes):
                Se esperan los dataframes en el orden mostrado y se asumen por defecto
                las tablas de BD en las que se carga la info:
                    Caso 1: tuple (len = 4): (opcd_df, opcdd_df, opbr_df, opbrd_df)
                    Caso 2: list (len = 4): [opcd_df, opcdd_df, opbr_df, opbrd_df]
                    Caso 3: 4 dataframes: opcd_df, opcdd_df, opbr_df, opbrd_df
                Se recibe cualquier cantidad de dataframes, relacionado tabla en BD con dataframe:
                    Caso 4: dict: Ejemplo:
                    {
                        opcd_df_table: opcd_df,
                        opcdd_df_table: opcdd_df,
                        opbr_df_table: opbr_df,
                        opbrd_df_table: opbrd_df,
                        ...
                    }

                Glosario:
                    opcd_df (dataframe): Contiene la informacion de publicacion de los
                    nodos de la superficie en deltas
                    opcd_df_table (str): nombre de la tabla en la que se va cargar opcd_df
                    opcdd_df (dataframe): Contiene la informacion de publicacion diaria
                    de la superficie en deltas
                    opcdd_df_table (str): nombre de la tabla en la que se va cargar opcdd_df
                    opbr_df (dataframe): Contiene la informacion de publicacion de los nodos
                    de la superficie en estrategias
                    opbr_df_table (str): nombre de la tabla en la que se va cargar opbr_df_df
                    opbrd_df (dataframe): Contiene la informacion de publicacion diaria de la
                    superficie en estrategias
                    opbrd_df_table (str): nombre de la tabla en la que se va cargar opbrd_df_df
        """
        try:
            logger.info("Creando objeto...")
            len_four = len(dataframes) == 4
            len_one = len(dataframes) == 1
            if not (len_four or len_one):
                raise_msg = "La cantidad de parametros no es correcta. "
                raise_msg += f"Se recibieron: {len(dataframes)}, Se esperaban: 1 o 4"
                raise PlataformError(raise_msg)
            if len_one:
                dataframes = dataframes[0]
            is_tuple = isinstance(dataframes, tuple)
            is_list = isinstance(dataframes, list)
            is_dict = isinstance(dataframes, dict)
            have_to_convert = is_tuple or is_list or len_four
            if have_to_convert:
                logger.info(
                    "Convirtiendo lista o tupla en diccionario y traduciendo columnas..."
                )
                self.dataframes_dict = self.convert_to_dict(dataframes)
                logger.info("Conversion en diccionario exitosa")
            elif is_dict:
                self.dataframes_dict = dataframes
            else:
                raise_msg = "El tipo de parametro recibido no es correcto. "
                raise_msg += "Se esperaba tuple, dict, list o 4 dataframes. "
                raise_msg += f"Se recibio: {type(dataframes)}"
                raise PlataformError(raise_msg)
            logger.info("Creancion de objeto exitosa")
        except (Exception,) as init_exc:
            logger.error(create_log_msg("Fallo la creacion del objeto"))
            raise PlataformError("Fallo la creacion del objeto") from init_exc

    def excute(self):
        """
        Ejecuta el proceso completo de publicacion:
            - Creacion de la conexion a la base de datos
            - Eliminacion de la informacion preexistente de la superficie en BD
            - Insercion de la informacion de la superficie en BD
            - Cierra la conexion a la BD

        """
        try:
            logger.info("Estableciendo conexion a la BD...")
            self.set_db_connection()
            logger.info("Conexion a la BD existosa")
            logger.info("Publicando dataframes...")
            self.publish_all()
            logger.info("Dataframes publicados exitosamente")
        except (Exception,) as exc_exc:
            error_msg = "Fallo el proceso de publicacion de la informacion "
            error_msg += "de superficies de opciones internacionales en BD published"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from exc_exc
        finally:
            if self.db_connection is None:
                self.db_connection.close()

    def convert_to_dict(self, dataframes):
        """
        Convierte una tupla o lista de dataframes en un diccionario.

        Args:
            dataframes (list o tuple)

        Returns:
            dict: Diccionario que relaciona las tablas en BD (keys) con los dataframes (values)
        """
        raise_msg = "Fallo la conversion a diccionario"
        try:
            len_dfs = len(dataframes)
            if len_dfs != 4:
                raise PlataformError("Se esperaban 4 dataframes, no " + str(len_dfs))
            PARAM_LIST = [
                "PUB_DB_OPCD_TABLE",  # deltas nodos
                "PUB_DB_OPCDD_TABLE",  # deltas diaria
                "PUB_DB_OPBR_TABLE",  # estrategias nodos
                "PUB_DB_OPBRD_TABLE",  # estrategias diaria
            ]
            PARAMS = get_params(PARAM_LIST)
            dfs_dict = {}
            for pos in range(0, len_dfs):
                table = PARAMS[PARAM_LIST[pos]]
                dfs_dict[table] = dataframes[pos]
            return dfs_dict
        except (Exception,) as ctd_exc:
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from ctd_exc

    def translate_df(self, table):
        """
        Traduce los nombres de los columnas de los dataframes a los usados en las tablas de la
        base de datos

        Args:
            df (dataframe): dataframe a traducir

        Returns:
            dataframe: datagframe con los nombres de columnas traducidos
        """
        raise_msg = "Fallo la traduccion del dataframe"
        try:
            PARAM_LIST = ["DELTAS_TRANSLATE_DICT", "STRATEGY_TRANSLATE_DICT"]
            PARAMS = get_params(PARAM_LIST)
            DELTAS_TRANSLATE_DICT = {"d":"days","X0.9":"X09","X0.75":"X075","X0.5":"X05","X0.25":"X025","X0.1":"X01","instrument":"currency"}
            STRATEGIES_TRANSLATE_DICT = json.loads(PARAMS["STRATEGY_TRANSLATE_DICT"])
            df_translated = self.dataframes_dict[table].rename(
                columns=DELTAS_TRANSLATE_DICT
            )
            df_translated = df_translated.rename(columns=STRATEGIES_TRANSLATE_DICT)
            self.dataframes_dict[table] = df_translated
        except (Exception,) as trans_exc:
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from trans_exc

    def set_db_connection(self):
        """
        Establece la conexion al esquema de base de datos de publicacion (pub_otc)
        """
        try:
            if self.db_connection is None:
                PARAM_LIST = ["CONFIG_SECRET_NAME", "PUB_DB_SECRET_KEY"]
                params = get_params(PARAM_LIST)
                config_secret_name = params["CONFIG_SECRET_NAME"]
                config_secret = get_secret(config_secret_name)
                db_secret_key = params["PUB_DB_SECRET_KEY"]
                db_secret_name = config_secret[db_secret_key]
                db_secret = get_secret(db_secret_name)
                self.db_connection = connect_db_by_secret(db_secret)
        except (Exception,) as db_exc:
            error_msg = "Fallo la creacion de la conexion al esquema de BD: pub_otc"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from db_exc

    def publish_all(self):
        """
        Publica la informacion de todos los dataframes (con los que se creo el objeto)
        en el esquema de base de datos de publicacion. Realiza:
            - Eliminacion de informacion preexistente en BD para la superficie a publicar
            - Inserta la informacion de la superficie en BD
        """
        try:
            for table in self.dataframes_dict:
                self.translate_df(table)
                df = self.dataframes_dict[table]
                if self.validate_df(table) is False:
                    error_msg = f"El dataframe que se quiere cargar en {table} no tiene una estructura valida"
                    raise PlataformError(error_msg)
                if "days" in df:
                    self.dataframes_dict[table].set_index("days", inplace=True)
                self.delete_repeated(table)
                self.insert(table)
        except (Exception,) as pub_exc:
            error_msg = (
                "Fallo la publicacion de los dataframes con la informacion de opt inter"
            )
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from pub_exc

    def delete_repeated(self, table):
        """
        Elimina la informacion preexistente en BD para la superficie a publicar. Identifca el dataframe
        a partir de la tabla (indexando el diccionario de dataframes del objeto). Identifica la
        informacion a eliminar a partir de los campos 'currency' y 'valuation_date' del dataframe

        Args:
            table (str): tabla de la BD en la que se va a eliminar informacion
        """
        raise_msg = f"Fallo la eliminacion de la informacion preexistente de la superficie en {table}"
        try:
            logger.info("Construyendo delete query...")
            df = self.dataframes_dict[table].reset_index()
            currency = df.at[0, "currency"]
            valuation_date_str = df.at[0, "valuation_date"]
            if not isinstance(valuation_date_str, str):
                date_type = type(valuation_date_str)
                raise PlataformError(
                    "Se esperaba valuation_date como tipo string, no como "
                    + str(date_type)
                )
            delete_info = f"currency: {currency} y valuation_date: {valuation_date_str}"
            delete_info += f" en {table}"
            delete_query = sql.sql.text(
                f"""DELETE FROM {table} WHERE currency = :CURRENCY 
                AND valuation_date = :VALUATION_DATE"""
            )
            logger.debug("Delete query: %s", delete_query)
            query_params = {
                "VALUATION_DATE": valuation_date_str,
                "CURRENCY": currency,
            }
            logger.info("delete query construida")
            logger.info("Eliminando informacion para %s", delete_info)
            self.db_connection.execute(delete_query, query_params)
            logger.info("Delete query ejecutada")
        except (Exception,) as del_exc:
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from del_exc

    def insert(self, table):
        """
        Inserta la informacion de la superficie en el esquema de base de datos de publish (pub_otc).

        Args:
            table (str): tabla en la que se va a publicar el dataframe. Se identifca el dataframe
            a partir de la tabla (indexando el diccionario de dataframes del objeto)
        """
        try:
            logger.info("Insertando dataframe en %s ...", table)
            self.dataframes_dict[table].to_sql(
                table, con=self.db_connection, if_exists="append"
            )
            logger.info("Inserción de dataframe exitosa")
        except (Exception,) as ins_exc:
            error_msg = "Fallo la insercion del dataframe en {table}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from ins_exc

    def validate_df(self, table):
        try:
            is_valid = True
            df = self.dataframes_dict[table]
            if df.isnull().values.any() == True:
                logger.info(
                    "El dataframe que se quiere cargar en %s tiene valores nulos", table
                )
                is_valid = False
            elif df.empty:
                logger.info("El dataframe que se quiere cargar en %s esta vacio", table)
                is_valid = False
            return is_valid
        except (Exception,) as val_exc:
            error_msg = (
                f"Fallo la validacion del dataframe que se quiere cargar en {table}"
            )
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from val_exc


try:
    params_key = [
        "DB_SECRET",
        "VALUATION_DATE",
        "CURRENCY",
        "PARAMETER_STORE"
    ]
    params_dict = get_params(params_key)
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url_sources = db_url_dict["conn_string_sources"]
    schema_sources = db_url_dict["schema_sources"]
    db_url_sources = db_url_sources + schema_sources
    parameter_store_name = params_dict["PARAMETER_STORE"]
    valuation_date = params_dict["VALUATION_DATE"]
    currency = params_dict["CURRENCY"]
    
    
    
    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc



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
        
    

    def get_data_opi(self):

        select_query = ("SELECT valuation_date, id_precia, currency, strategy, tenor, bid, ask, MID, maturity_date "+
                        "FROM src_otc_options_inter "+
                        f"WHERE valuation_date = '{valuation_date}' AND currency = '{currency}' and status_info = 1"
        )
        db_connection = self.create_connection()
        fwd_params_df = pd.read_sql(
            select_query, db_connection
        )
        logger.debug(select_query)
        db_connection.close()
        return fwd_params_df 
        

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
          "output_id":[currency],
          "process": "Derivados OTC",
          "product": "opt_inter",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-job-p-process-otc-opt-inter",
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



class OptInterApp:
    """
    Clase que orquesta el procesamiento y publicacion de opciones internacionales
    """

    def __init__(self) -> None:
        self.input_file_path = "DESCONOCIDO"
        self.job_name = "DESCONOCIDO"

    def get_config_process(self) -> dict:
        """
        Trae los datos de configuracion necesario para el proceso

        Returns:
            dict: diccionario de configutacion de optimus K
        """
        error_msg = "Fallo en obtener todos los datos de configuracion"
        try:
            logger.info("Obteniendo la configuracion del proceso ...")
            params_key = [
                "CONFIG_SECRET_NAME",
                "OPT_INTER_INSTRUMENTS",
                "JOB_NAME",
                "VALUATION_DATE"
            ]
            params_dict = get_params(params_key)
            config_secret_name = params_dict["CONFIG_SECRET_NAME"]
            config_secret = get_secret(config_secret_name)
            valuation_date = params_dict["VALUATION_DATE"]
            
            
            
            self.job_name = params_dict["JOB_NAME"]
            logger.info("Nombre Glue Job: %s", self.job_name)
            try:
                datetime.strptime(valuation_date, "%Y-%m-%d")
            except ValueError:
                valuation_date = datetime.now().strftime("%Y-%m-%d")
            logger.info("fecha de valoracion: %s", valuation_date)
            logger.info("Configuracion cargada")
            return config_secret
        except KeyError as key_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError("Secreto de configuracion incompleto") from key_exc
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def load_input_data(self) -> object:
        """
        Extrae, limpia  y carga en base de datos la informacion fel archivo insumo de refinitiv
        cargado en el bucket S3 configurado en secreto de configuracion
        Returns:
            object (pd.DataFrame): Datos del archivo limpios
        """
        error_msg = "Fallo la obtencion de los datos del archivo insumo"
        try:
            logger.info("Extrayendo, limpiando y cargando datos del archivo insumo ...")

            actionsdb = actions_db(db_url_sources)
            input_data = actionsdb.get_data_opi()
            input_data['maturity_date'] = None
            logger.debug("Datos del archivo insumo: \n %s", input_data.to_string())
            logger.info("Datos del archivo insumo listos para usar")
            return input_data
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def get_fwd_inter_data(self, config_secret: dict) -> object:
        """
        Obtiene datos del derivado fwd inter para el instrumento especificado si este
        es incluido en el secreto de configuracion de optimus K bajo la llave
        'dependence/fwd_to_opt'

        Args:
            config_secret (dict): Secreto de configuracion de optimus K

        Returns:
            object: Dataframe pandas con la informacion solicitada
        """
        error_msg = "No fue posible obtener datos del forward internacional solicitado"
        error_msg += f" {currency} para {valuation_date}"
        try:
            logger.info(
                "Obteniendos datos dependientes del proceso de forward internacional ..."
            )
            fwd_inter = ForwardNode(currency, valuation_date, config_secret)
            fwd_inter_data = fwd_inter.get_maturity_date()
            if fwd_inter_data.empty:
                logger.info(
                    "No hay dependencias asociadas al instrumento %s", currency
                )
            else:
                logger.info(
                    "Datos de los nodos forward internacional asociado: \n %s",
                    fwd_inter_data.to_string(),
                )
                tenor_list = fwd_inter_data["tenor"].tolist()
                logger.info('Tenores consultados de fwd para obtener las fehcas de maduración: '+ str(tenor_list))
                if "2Y" in tenor_list:
                    logger.debug('Se encuentran los tenores completos')
                else:
                    fwd_inter_data_2 = fwd_inter_data[fwd_inter_data["tenor"]=='1Y']
                    days = 365
                    fwd_inter_data_2['days'] = int(days) + int(days)
                    fwd_inter_data_2['tenor'] = "2Y"
                    fwd_inter_data["maturity_date"] = pd.to_datetime(fwd_inter_data["maturity_date"], format="%Y-%m-%d")
                    fwd_inter_data_2["maturity_date"] = fwd_inter_data["maturity_date"] + timedelta(days=int(days))
                    fwd_inter_data = pd.concat([fwd_inter_data, fwd_inter_data_2])
            
            return fwd_inter_data
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def process(self, input_data, fwd_data) -> dict:
        """
        Procesa los datos limpios del archivo insumo para obtener 4 dataframes pandas
        y ordenarlos en un diccionario para su publicacion

        Args:
            input_data (DataFrame): Datos del archivo insumo limpios
            fwd_data (DataFrame): Datos de fordward internacional para el mismo

        Returns:
            dict: Datos a publicar
        """
        error_msg = "Fallo el proceso de negocio de opciones internacionales"
        try:
            logger.info("Iniciando logica de negocio de opciones internacionales ...")
            opt_inter_process = VolatilitySurface(
                input_data, fwd_data, valuation_date, currency
            )
            opt_inter_process.all_surface_df()
            all_out_data = {
                "OPBR": opt_inter_process.int_surface_db,
                "OPCD": opt_inter_process.int_surface_deltas,
                "OPBRD": opt_inter_process.daily_int_surface,
                "OPCDD": opt_inter_process.daily_int_surface_deltas,
            }
            logger.info("Logica de negocio de opciones internacionales finalizada")
            return all_out_data
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def publish(self, processed_data: dict) -> None:
        """
        Publica los datos entregados por la logica de negocio en base de datos y FTP

        Args:
            processed_data (dict): datos entregados por el proceso

        Raises:
            PlataformError: Cuando el diccionario de los dataframe a publicar incompleto
        """
        error_msg = "Diccionario de los dataframe a publicar incompleto"
        try:
            opbr_df = processed_data["OPBR"]
            opcd_df = processed_data["OPCD"]
            opbrd_df = processed_data["OPBRD"]
            opcdd_df = processed_data["OPCDD"]
        except KeyError as key_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from key_exc

        error_msg = "Fallo al publicar en la base de datos"
        try:
            logger.info("Publicando en base de datos ...")
            publisher_to_db = PublishInBD(opcd_df, opcdd_df, opbr_df, opbrd_df)
            publisher_to_db.excute()
            logger.info("Finaliza publicacion en base de datos")
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

        error_msg = "Fallo al publicar en el FTP"
        try:
            logger.info("Publicando en el FTP ...")
            publisher_to_ftp = FileManager(
                currency, valuation_date, opcdd_df, opcd_df, opbrd_df, opbr_df
            )
            publisher_to_ftp.run()
            logger.info("Finaliza publicacion en el FTP")
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def process_data(self) -> None:
        """
        Agrupa todas las funcionalidades que permiten convertir los datos del archivo
        insumo en los datos que se publican por diferentes canales
        """
        error_msg = "Error durante el proceso y publicacion de los datos"
        try:
            config_secret = self.get_config_process()
            fwd_data = self.get_fwd_inter_data(config_secret)
            input_data = self.load_input_data()
            deltas_strategies_data = self.process(input_data, fwd_data)
            self.publish(deltas_strategies_data)
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def update_status(self, status: str):
        """
        Actualiza el estatus del instrumento para orquestar la validacion conjunta

        Args:
            status (str): Estatus a actualizar
        """
        error_msg = "Fallo al actualizar el status"
        try:
            report = StatusReport()
            report.update_status(status, currency, valuation_date)
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def send_error_mail(self, error_msg):
        """
        Envia el correo de error

        Args:
            error_msg (str): Mensaje de erro para el usuario
        """
        subject = "Optimus-K: Error al procesar Opciones Internacionales "
        subject += f"{currency} {valuation_date}"
        body = f"""
Cordial saludo.

Durante la el procesamiento del archivo insumo Fenics opciones el Glue Job '{self.job_name}' presento el siguiente error.

Error comunicado:
{error_msg}


Puede encontrar el log para mas detalles de las siguientes maneras:

    - Ir al servicio AWS Glue desde la consola AWS, dar clic en 'Jobs'(ubicado en el menu izquierdo), dar clic en el job informado en este correo, dar clic en la pestaña 'Runs' (ubicado en la parte superior), y buscar la ejecucion asociada a este mensaje por instrumento, fecha de valoracion y error comunicado, para ver el log busque el apartado 'Cloudwatch logs' y de clic en el link 'Output logs'.

    - Ir al servicio de CloudWatch en la consola AWS, dar clic en 'Grupos de registro' (ubicado en el menu izquierdo), y filtrar por '/aws-glue/python-jobs/output' e identificar la secuencia de registro por la hora de envio de este correo electronico.



Optimus-K

Enviado por el servicio de notificaciones de Precia PPV S.A. en AWS
        """
        error_email = "Fallo al enviar el email de error"
        try:
            logger.info("Enviando email de error ...")
            ReportEmail(subject, body, {})
            logger.info("Email de error enviando exitosamente")
        except (Exception,):
            logger.error(create_log_msg(error_email))
            raise

    def report_error(self, msg_to_user: str) -> None:
        """
        Reporta en base de datos y por correo el error identificado

        Args:
            msg_to_user (str): Mensaje de error que ve el ususario en el correo
        """

        error_msg = "Fallo al actualizar el estatus Fallido"
        try:
            self.update_status("Fallido")
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            msg_to_user += f", {error_msg}"

        error_msg = "Fallo el envio de correo electronico de error"
        try:
            self.send_error_mail(msg_to_user)
        except (Exception,):
            logger.error(create_log_msg(error_msg))

    def execute(self) -> None:
        """
        Orquesta todos los procesos para opciones internacinacionales
        """
        error_msg = "Glue Job interrumpido"
        try:
            logger.info("Inicia el Glue Job ...")
            self.process_data()
            self.update_status("Exitoso")
            logger.info("Finaliza el Glue Job exitosamente")
            update_report_process("Exitoso", "Proceso Finalizado", "")
        except DependencyError:
            logger.critical(create_log_msg(error_msg))
            update_report_process("Fallido", error_msg, str(DependencyError))
            raise
        except PlataformError as known_exc:
            logger.critical(create_log_msg(error_msg))
            msg_to_user = str(known_exc)
            self.report_error(msg_to_user)
            msg_to_user = f"{currency}-{valuation_date}:{msg_to_user}"
            update_report_process("Fallido", error_msg, str(known_exc))
            raise PlataformError(msg_to_user) from known_exc
        except (Exception,) as unknown_exc:
            logger.critical(create_log_msg(error_msg))
            msg_to_user = "Error desconocido, revise el log asociado y considere solicitar soporte"
            self.report_error(msg_to_user)
            msg_to_user = f"{currency}-{valuation_date}: {msg_to_user}"
            update_report_process("Fallido", error_msg, str(unknown_exc))
            raise PlataformError(msg_to_user) from unknown_exc



if __name__ == "__main__":
    app = OptInterApp()
    app.execute()
