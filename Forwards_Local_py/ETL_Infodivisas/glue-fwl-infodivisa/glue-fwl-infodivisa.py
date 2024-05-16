"""
===============================================================================

Nombre: glue-dev-fwl-infodivisa.py

Tipo: Glue Job

Autor:
    - Hector Augusto Daza Roa
Tecnología - Precia

Ultima modificacion: 08/02/2023

Este el script principal de la ETL de fwd local ICAP: Procesa el insumo
almacenado en el bucket de S3 (AAAAMMDD- infodivisa.xlsx), extrae la
informacion de interes y la carga en el esquema de base de datos de src_otc. Se
ejecuta el proceso principal instanciando las clases para gestionar archivos
(InputManager), hacer consultas en base de datos (DBManger) y llevar a cabo el
proceso de extraccion y tranformacion de la informacion del insumo
(EtlIcapFwdLocal) 

Parametros del Glue Job:
"--TENOR"= "ON,1W,2W,1M,2M,3M,6M,9M,1Y,18M"
"--FORMAT_DATE"= "%Y-%m-%d"
"--NAME_TABLE"= "src_otc_fwd_local_brokers"
"--VALORATION_PATH"= "otc/forward/local/valoracion/"
"--DECIMAL_ROUND"= "3"
"--S3_FILE_PATH"= "otc/forward/local/20230131- infodivisa.xlsx" -> viene del
lambda launcher
"--SECRET_DB"= "precia/aurora/optimusk/sources/url"
"--NAME_COLUMNS"= "{'Plazo': 'days','COMPRA': 'bid','VENTA': 'ask','MEDIO (t)':
'mid_fwd'}"
"--BROKER"= "ICAP"
"--VALID_COLUMNS"= "Plazo,COMPRA,VENTA,MEDIO (t)"
"--PRICES_TYPES"= "COMPRA,VENTA"
"--MEGATRON_PATH"= "otc/forward/local/"
"--S3_BUCKET"= "s3-dev-datalake-sources"

===============================================================================
"""

import json
import logging
from datetime import datetime as dt

import pandas as pd
import numpy as np
import sqlalchemy as sa
import boto3

from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret

logger = setup_logging(logging.INFO)


class InputManager:
    """Gestiona la descarga del insumo, su conversion a dataframe y la obtencion
    de metadata (nombre,fecha,ruta,autor)
    """

    def __init__(
        self,
        filepath: str,
        date_format: str,
        megatron_path: str,
        valoration_path: str,
        bucket: str,
        file_size_max: int,
    ) -> None:
        """
        Gestiona la descarga del insumo, su conversion a dataframe y la
        obtencion de metadata (nombre,fecha,ruta,autor)

        Args:
            filepath (str): Ruta del archivo en el bucket de S3
            date_format (str): Formato para la fecha
            megatron_path (str): Ruta en el bucket de S3 que corresponde a la
            coleccion automatica de Megatron
            valoration_path (str): Ruta en el bucket de S3 que corresponde a la
            carga manual de insumos por parte de valoracion
            bucket (str): Bucket de S3 de donde se descarga el insumo
            file_size_max (int): maximo tamanio de archivo aceptado

        Raises:
            PlataformError: Cuando falla la creacion del objeto InputManager
        """ 
        try:
            self.filepath = filepath
            self.file_size_max = file_size_max
            self.date_format = date_format
            self.megatron_path = megatron_path
            self.valoration_path = valoration_path
            self.bucket = bucket
            self.filename = self.get_filename()
            self.valuation_date = self.get_valuation_date()
            self.author = self.get_author()
            self.file = self.download_from_bucket()
            self.input_df = pd.read_excel(self.file)
        except (Exception,) as init_exc:
            raise_msg = "Fallo la instanciacion del objeto InputManager"
            raise_msg += f". Insumo: {filepath}"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from init_exc

    def download_from_bucket(self) -> bytes:     
        """Descarga el insumo desde el buket en la memoria del Glue Job

        Raises:
            PlataformError: Cuando falla la descarga del insumo
            PlataformError: Caundo el tamnio del archivo sumera el maximo
            aceptado

        Returns:
            bytes: Insumo en memoria del Glue Job
        """
        try:
            s3 = boto3.client("s3")
            logger.info("Obteniendo objeto de S3")
            s3_object = s3.get_object(Bucket=self.bucket, Key=self.filepath)
            logger.debug(f"object:{s3_object}")
            file_size = s3_object["ContentLength"]
            logger.info(f"El tamanio del archivo es: {file_size} bytes")
            if file_size > self.file_size_max:
                logger.info(
                    f"El archivo {self.filename} es sospechosamente grande, no sera procesado."
                )
                raise_msg = (
                    f"El archivo {self.filename} ({file_size}) supera el tamanio maximo aceptado. "
                )
                raise_msg += f"Maximo: {self.file_size_max}B"
                raise PlataformError(raise_msg)

            input_file = s3_object["Body"].read()
            logger.info("Insumo descargado correctamente en la memoria del Glue Job")
            return input_file
        except (Exception,) as file_exc:
            error_msg = f"Fallo la obtencion del insumo {self.filename} desde el bucket {self.bucket}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from file_exc

    def get_filename(self) -> str:
        """Extrae el nombre del Insumo a partir del string con la ruta completa

        Returns:
            str: Nombre del insumo
        """
        try:
            logger.info("Extrayendo el nombre del insumo...")
            filename = self.filepath.split("/")[-1]
            logger.info(f"Insumo: {filename}")
            return filename
        except (Exception,) as file_exc:
            error_msg = f"Fallo la obtencion del nombre insumo: {self.filepath}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from file_exc

    def get_valuation_date(self) -> str:
        """Extrae la fecha de valoracion a partir del nombre del insumo

        Raises:
            PlataformError: Cuando la fecha de valoracion no coincide con la
            fecha de ejecucion
            PlataformError: Cuando la extraccion de la fecha
            de valoracion falla

        Returns:
            str: Fecha de valoracion del insumo
        """
        try:
            valuation_date_str = self.filename.split("-")[0]
            valuation_date = dt.strptime(valuation_date_str, "%Y%m%d")
            valuation_date_str = valuation_date.strftime(self.date_format)
            today_date = dt.now().date()
            today_date_str = today_date.strftime(self.date_format)
            logger.info("Validando fecha de valoracion...")
            if valuation_date_str != today_date_str:
                raise_msg = (
                    f"La fecha de valoracion del insumo ({valuation_date_str}) no "
                )
                raise_msg += f"corresponde con la fecha de ejecucion de la etl ({today_date_str})"
                logger.warning(raise_msg)
            logger.info(f"Fecha de valoracion: {valuation_date_str}")
            return valuation_date_str
        except (Exception,) as date_exc:
            error_msg = f"Fallo la obtencion de la fecha de valoracion del insumo: {self.filepath}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from date_exc

    def get_author(self) -> str:
        """Identifica el autor del insumo:

        Raises:
            PlataformError: Cuando falla la identificacion del autor

        Returns:
            str:
                'MEGATRON': Si el insumo proviene de la recoleccion automatica
                'VALORACION': Si el insumo fue editado y cargado manualmente
                por un analista de valoracion
        """
        try:
            path = self.filepath.replace(self.filename, "")
            if path == self.megatron_path:
                author = "MEGATRON"
            elif path == self.valoration_path:
                author = "VALORACION"
            return author
        except (Exception,) as aut_exc:
            error_msg = f"Fallo lidentificacion del autor del insumo: {self.filepath}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from aut_exc


class DBManager:
    """Gestiona la conexion y las transacciones a base de datos"""

    def __init__(self, db_url: str) -> None:
        """Gestiona la conexion y las transacciones a base de datos

        Args:
            db_url (str): url necesario para crear la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url = db_url
        except (Exception,) as init_exc:
            error_msg = f"Fallo la creacion del objeto DBManager"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from init_exc

    def insert_df(self, info_df: pd.DataFrame, db_table: str):
        """Inserta el dataframe dado en la tabla de base de datos indicada

        Args:
            info_df (pd.DataFrame): dataframe a insertar
            db_table (str): tabla de base de datos

        Raises:
            PlataformError: cuando la insercion del dataframe falla
        """
        try:
            logger.info("Insertando dataframe en BD...")
            db_connection = self.create_connection()
            info_df.to_sql(
                db_table, con=db_connection, if_exists="append"
            )
            logger.info("Intentando cerrar la conexion a BD")
            db_connection.close()
            logger.info("Conexion a BD cerrada con exito")
            logger.info("Insersion de dataframe en BD exitosa")
        except (Exception,) as ins_exc:
            raise_msg = "Fallo la insercion en BD"
            logger.error(create_log_msg(raise_msg))
            try:
                logger.info("Intentando cerrar la conexion a BD")
                db_connection.close()
            except (Exception,):
                error_msg = "Fallo el intento de cierre de conexion a BD"
                logger.error(create_log_msg(error_msg))
            raise PlataformError() from ins_exc

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
                self.db_url, connect_args={"connect_timeout": 2}
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

    def disable_previous_info(self, info_df: pd.DataFrame, db_table: str):
        """Deshabilita la informacion anterior que coincida con el broker y la
        fecha de valoracion del dataframe dado

        Args:
            info_df (pd.DataFrame): dataframe de referencia
            db_table (str): tabla en base de datos

        Raises:
            PlataformError: cuando la deshabilitacion de informacion falla
        """
        try:
            logger.info("Construyendo query de actualizacion...")
            valuation_date_str = info_df.at["ON", "valuation_date"]
            broker = info_df.at["ON", "broker"]
            update_query = sa.sql.text(
                f"""UPDATE {db_table}
                SET status_info= :status_info WHERE broker = :broker 
                and valuation_date = :valuation_date
                """
            )
            query_params = {
                "status_info": 0,
                "broker": broker,
                "valuation_date": valuation_date_str,
            }
            logger.info("Query de actualizacion construido con exito")
            db_connection = self.create_connection()
            logger.info("Ejecutando query de actualizacion...")
            db_connection.execute(update_query, query_params)
            logger.info("Query de actualizacion ejecutada con exito")
            logger.info("Intentando cerrar la conexion a BD")
            db_connection.close()
            logger.info("Conexion a BD cerrada con exito")
        except (Exception,) as ins_exc:
            raise_msg = "Fallo la deshabilitacion de informacion en BD"
            logger.error(create_log_msg(raise_msg))
            try:
                logger.info("Intentando cerrar la conexion a BD")
                db_connection.close()
            except (Exception,):
                error_msg = "Fallo el intento de cierre de conexion a BD"
                logger.error(create_log_msg(error_msg))
            raise PlataformError() from ins_exc


class EtlIcapFwdLocal:
    """Extrae la informacion de interes del dataframe del insumo (input_df), la
    transforma y la almacena en un dataframe de salida (output_df) listo
    para cargar en BD
    """

    def __init__(
        self,
        broker: str,
        tenores: list,
        columns_of_interest: list,
        translate_dict: dict,
        prices_types: list,
        input_df: pd.DataFrame,
        valuation_date: str,
        author: str,
        decimal_round: int,
    ) -> None:
        """
        Extrae la informacion de interes del dataframe del insumo (input_df),
        la transforma y la almacena en un dataframe de salida (output_df) listo
        para cargar en BD

        Args:
            broker (str): proveedor del insumo de informacion
            tenores (list): siglas que representan los plazos de vencimiento de
            puntos forward local
            columns_of_interest (list): columnas que se extraeran del insumo
            translate_dict (dict): dicciorio para traducir los nombres de las
            columnas
            prices_types (list): lista de nombres que corresponden con bid y ask
            input_df (pd.DataFrame): dataframe con la informacion completa del
            insumo (antes de filtrar)
            valuation_date (str): fecha de valoracion del insumo
            author (str): autor del insumo
            decimal_round (int): numero de cifras decimales a redondear en los
            precios
        """
        self.broker = broker
        self.tenores = tenores
        self.decimal_round = decimal_round
        self.columns_of_interest = columns_of_interest
        self.valuation_date = valuation_date
        self.translate_dict = translate_dict
        self.prices_types = prices_types
        self.input_df = input_df
        self.author = author
        self.output_df = self.run_etl()

    def filter_prices_columns(
        self, fwd_col: int, process_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Elimina las columnas de precios sobrantes del dataframe dado para los
        tipos de precios especificados. Primero valida las celdas de precios
        que corresponden a forward (deben corresponder con bid, ask), luego
        elimina todas las demas

        Args:
            fwd_col (int): Columna en la que se encuentra la celda con el
            encabezado de puntos forward
            process_df (pd.DataFrame): Dataframe a filtrar

        Raises:
            PlataformError: Cuando la primera celda debajo de la celda de
            puntos forward no corresponde ni con bid ni con ask
            PlataformError: Cuando la segunda celda debajo de la celda de
            puntos forward no corresponde ni con bid ni con ask
            PlataformError: Cuando falla el filtro sobre las columnas de
            precios

        Returns:
            pd.DataFrame: Dataframe filtrado
        """
        try:
            # Validacion de la celdas de precios que corresponden a forward
            # Las celdas abajo de la celda de puntos fwd deben corresponder con bid, ask
            first_fwd_header_col = fwd_col
            second_fwd_header_col = first_fwd_header_col + 1
            first_fwd_header = process_df.iat[0, first_fwd_header_col]
            second_fwd_header = process_df.iat[0, second_fwd_header_col]
            raise_msg = "El valor de la celda no era el esperado"
            raise_msg += f". Se esperaba uno de los siguientes: {self.prices_types}"
            prices_types_copy = self.prices_types.copy()
            if first_fwd_header in prices_types_copy:
                prices_types_copy.remove(first_fwd_header)
            else:
                raise_msg += f". Valor actual de la celda: {first_fwd_header}"
                raise_msg += f". Posicion de la celda: {[0, first_fwd_header_col]}"
                raise PlataformError(raise_msg)
            if second_fwd_header not in prices_types_copy:
                raise_msg += f". Valor actual de la celda: {second_fwd_header}"
                raise_msg += f". Posicion de la celda: {[0, first_fwd_header_col+1]}"
                raise PlataformError(raise_msg)

            # Identificando columnas con celdas que corresponden a precios de bid y ask
            purchase_cols = np.where(process_df.eq(self.prices_types[0]))[1]
            sale_cols = np.where(process_df.eq(self.prices_types[1]))[1]

            # Elimnando columnas de precios sobrantes
            cols_to_drop = np.concatenate((purchase_cols, sale_cols))
            cols_to_drop = [
                x
                for x in cols_to_drop
                if x not in [first_fwd_header_col, second_fwd_header_col]
            ]
            cols_to_drop = [process_df.columns[i] for i in cols_to_drop]
            process_df = process_df.drop(columns=cols_to_drop)
            logger.debug(
                f"Dataframe despues de eliminacion de columnas de precio repetidas:\n{process_df}"
            )
            return process_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la eliminacion de las columnas de precio sobrantes"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc

    def extract_dataframe(self, process_df) -> pd.DataFrame:
        """Extrae un dataframe con la informacion de interes del dataframe dado

        Args:
            process_df (_type_): Dataframe a filtrar

        Raises:
            PlataformError: Cuando el dataframe viene sin celda "PUNTOS FORWARD*"
            PlataformError: Cuando falla la extraccion de la informacion de interes

        Returns:
            pd.DataFrame: Dataframe que contiene la informacion de interes
        """
        try:
            init_df_msg = (
                f"Dataframe original (antes de cualquier transformacion):\n{process_df}"
            )
            logger.debug(init_df_msg)
            logger.info('Validando celda de "PUNTOS FORWARD*"...')
            fwd_row, fwd_col = np.where(process_df.eq("PUNTOS FORWARD*"))
            if len(fwd_row) != 1 or len(fwd_col) != 1:
                raise_msg = 'El insumo contiene multiples celdas "PUNTOS FORWARD*"'
                raise_msg += " o ninguna (verifique la integridad del insumo)"
                raise PlataformError(raise_msg)
            logger.info('Celda de "PUNTOS FORWARD*" validada con exito')

            # Eliminando filas sobrantes
            logger.info("Eliminando filas sobrantes...")
            process_df = process_df.iloc[fwd_row[0] + 1 : fwd_row[0] + 12]
            logger.debug(
                f"Dataframe despues de eliminar filas sobrantes:\n{process_df}"
            )

            # Eliminado columnas de precios repetidas
            logger.info(
                f"Eliminado columnas de precios {self.prices_types} repetidas ..."
            )
            fwd_col = fwd_col[0]
            process_df = self.filter_prices_columns(
                process_df=process_df, fwd_col=fwd_col
            )

            # conviertiendo fila de titulos en encabezado del df
            process_df.columns = process_df.iloc[0]
            process_df = process_df.iloc[1:]

            # Eliminando columnas sobrantes
            process_df = process_df.loc[:, self.columns_of_interest]
            logger.debug(f"Dataframe extraido del insumo:\n{process_df}")
            return process_df
        except (Exception,) as ext_exc:
            raise_msg = "Fallo la extraccion del dataframe desde el insumo"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from ext_exc

    def add_columns(self, process_df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega las siguientes columnas 'tenor','broker', 'valuation_date y
        'author' al dataframe dado

        Args:
            process_df (pd.DataFrame): Dataframe al que se le van a agregar las
            columnas

        Raises:
            PlataformError: Cuando falla la adicion de columnas

        Returns:
            pd.DataFrame: Dataframe con las columnas agregadas
        """
        try:
            process_df["tenor"] = self.tenores
            process_df["broker"] = self.broker
            process_df["author"] = self.author
            process_df["valuation_date"] = self.valuation_date
            return process_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la adicion de columnas"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc

    def run_etl(self) -> pd.DataFrame:
        """
        Ejecuta el proceso completo de la clase de la ETL Infodivisas fwd
        local:
            - Extraccion
            - Traduccion
            - Ajuste de nodo ON
            - Adicion de columnas
            - Redondeo de precios

        Raises:
            PlataformError: cuando el proceso de la ETL Infodivisas fwd local
            falla

        Returns:
            pd.DataFrame: Dataframe de salida despues de pasar por todo el
            proceso de la ETL
        """
        try:
            # Extraccion
            logger.info("Extrayendo dataframe con solo la informacion de interes...")
            process_df = self.extract_dataframe(process_df=self.input_df)
            logger.info(
                "Dataframe con solo la informacion de interes obtenido con exito"
            )

            # Traduccion de terminos: insumo -> BD
            logger.info("Traduciendo los nombres de las columnas...")
            process_df = process_df.rename(columns=self.translate_dict)
            logger.info("Nombres de las columnas traducidos con exito")

            # Ajuste nodo ON
            logger.info("Ajustando nodo overnight...")
            process_df.replace({"OVERNIGHT": 1}, inplace=True)
            process_df.sort_values(by="days", inplace=True)
            logger.debug(f"Dataframe despues de ajuste de nodo ON:\n{process_df}")

            # Adicion de columnas faltantes
            logger.info("Agregando columnas faltantes...")
            process_df = self.add_columns(process_df=process_df)
            logger.debug(f"Dataframe despues de agregar columnas:\n{process_df}")

            # Redondeo
            logger.info("Redondeando precios...")
            process_df.loc[:, ("mid_fwd", "bid", "ask")] = round(
                process_df.loc[:, ("mid_fwd", "bid", "ask")], self.decimal_round
            )
            logger.debug(f"Dataframe despues de redondeo:\n{process_df}")

            process_df.set_index("tenor", inplace=True)
            logger.debug(f"Dataframe listo para cargar en BD:\n{process_df}")

            return process_df
        except (Exception,) as etl_exc:
            raise_msg = (
                "Fallo el procesamiento de la clase de ETL Infodivisas fwd local"
            )
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from etl_exc


# Obtencion de parametros del Glue Job
try:
    # Parametros
    params_keys = [
        "S3_BUCKET",
        "SECRET_DB",
        "BROKER",
        "NAME_TABLE",
        "TENOR",
        "FORMAT_DATE",
        "DECIMAL_ROUND",
        "VALORATION_PATH",
        "MEGATRON_PATH",
        "S3_FILE_PATH",
        "NAME_COLUMNS",
        "VALID_COLUMNS",
        "PRICES_TYPES",
    ]
    params_dict = get_params(params_keys)
    S3_FILE_PATH = params_dict["S3_FILE_PATH"]
    NAME_COLUMNS = json.loads(params_dict["NAME_COLUMNS"].replace("'", '"'))
    BROKER = params_dict["BROKER"]
    VALID_COLUMNS = params_dict["VALID_COLUMNS"].split(",")
    PRICES_TYPES = params_dict["PRICES_TYPES"].split(",")
    S3_BUCKET = params_dict["S3_BUCKET"]
    SECRET_DB = params_dict["SECRET_DB"]
    VALORATION_PATH = params_dict["VALORATION_PATH"]
    MEGATRON_PATH = params_dict["MEGATRON_PATH"]
    NAME_TABLE = params_dict["NAME_TABLE"]
    TENOR = params_dict["TENOR"].split(",")
    FORMAT_DATE = params_dict["FORMAT_DATE"]
    DECIMAL_ROUND = int(params_dict["DECIMAL_ROUND"])
    # Secretos
    DB_URLS_DICT = get_secret(SECRET_DB)
    DB_URL_SECRET = DB_URLS_DICT["conn_string_sources"]

    FILE_SIZE_MAX = 5e7

except (Exception,) as init_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from init_exc

if __name__ == "__main__":
    """Orquesta las clases de la etl, las consultas a BD y la lectura del insumo"""
    try:
        logger.info("Descargando el insumo y extrayendo su metainfo...")
        input_manager = InputManager(
            filepath=S3_FILE_PATH,
            date_format=FORMAT_DATE,
            valoration_path=VALORATION_PATH,
            megatron_path=MEGATRON_PATH,
            bucket=S3_BUCKET,
            file_size_max=FILE_SIZE_MAX,
        )
        logger.info("Insumo descargado y metainfo extraida exitosamente")
        logger.info(
            "Iniciando la extracccion y transformacion de informacion del insumo..."
        )
        etl = EtlIcapFwdLocal(
            broker=BROKER,
            tenores=TENOR,
            columns_of_interest=VALID_COLUMNS,
            translate_dict=NAME_COLUMNS,
            prices_types=PRICES_TYPES,
            input_df=input_manager.input_df,
            author=input_manager.author,
            valuation_date=input_manager.valuation_date,
            decimal_round=DECIMAL_ROUND,
        )
        logger.info("Extracccion y transformacion de informacion del insumo exitosas")
        db_manager = DBManager(db_url=DB_URL_SECRET)
        logger.info("Deshabilitando informacion anterior en BD...")
        db_manager.disable_previous_info(info_df=etl.output_df, db_table=NAME_TABLE)
        logger.info("Informacion anterior deshabilitada en BD exitosamente")
        logger.info("Cargando nueva informacion en BD...")
        db_manager.insert_df(info_df=etl.output_df, db_table=NAME_TABLE)
        logger.info("Nueva informacion en BD cargada exitosamente")
        logger.info(
            "Finaliza el proceso completo de la ETL fwd local ICAP exitosamente"
        )
    except (Exception,):
        raise_msg = "Fallo el proceso de extraccion, transformacion"
        raise_msg += f" y carga de la informacion del insumo: {S3_FILE_PATH}"
        logger.error(create_log_msg(raise_msg))
