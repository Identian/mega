"""
===============================================================================

Nombre: glue-dev-fwl-gfi-snrd.py

Tipo: Glue Job

Autor:
    - Lorena Julieth Torres Hernandez
Tecnología - Precia

Ultima modificacion: 03/02/2023

Este el script principal de la ETL de fwd local GFI: Procesa el insumo
almacenado en el bucket de S3, extrae la informacion de interes y la carga en
el esquema de base de datos de src_otc.

Parametros del Glue Job:
"--S3_FILE_PATH" = otc/forward/local/Formato envío FP por los SNRD-170123.xlsx
"--BROKER" = GFI
"--SRC_NAME_TABLE" = src_otc_fwd_local_brokers
"--FORMAT_DATE" = %Y-%m-%d
"--NEW_COLUMNS" = {"Numéro de días": "days","Compra/Bid": "bid","Venta/Offer":
"ask"}
"--TENOR" = 1W,2W,1M,2M,3M,6M,9M,1Y,18M
"--MAX_NULL_DATA" = 17
"--ORDER_DF" = broker,Tenor,days,mid_fwd,bid,ask,valuation_date,author
"--VALUATION_PATH" = VALORACION
"--S3_BUCKET"=s3-dev-datalake-sources
"--SECRET_DB"=precia/aurora/optimusk/sources/url

===============================================================================
"""

import logging
import boto3
import io
import json
import pandas as pd
import sqlalchemy as sa
import sys
from datetime import datetime
from precia_utils.precia_exceptions import PlataformError, UserError
from precia_utils.precia_logger import setup_logging, create_log_msg, create_log_msg
from precia_utils.precia_aws import get_params, get_secret

logger = setup_logging(logging.INFO)
MAX_FILE_SIZE = 5e7
try:
    params_key = [
        "TENOR",
        "BROKER",
        "SRC_NAME_TABLE",
        "FORMAT_DATE",
        "NEW_COLUMNS",
        "MAX_NULL_DATA",
        "ORDER_DF",
        "S3_FILE_PATH",
        "VALUATION_PATH",
        "BUCKET_NAME",
        "SECRET_DB",
    ]
    params_dict = get_params(params_key)
    tenor = params_dict["TENOR"]
    tenor = tenor.split(",")
    broker = params_dict["BROKER"]
    table = params_dict["SRC_NAME_TABLE"]
    format_date = params_dict["FORMAT_DATE"]
    new_columns = params_dict["NEW_COLUMNS"]
    new_columns = json.loads(new_columns)
    max_null_data = int(params_dict["MAX_NULL_DATA"])
    path = params_dict["S3_FILE_PATH"]
    valuation_path = params_dict["VALUATION_PATH"]
    order_df = params_dict["ORDER_DF"]
    order_df = order_df.split(",")
    Bucket_name = params_dict["BUCKET_NAME"]
    secret_db = params_dict["SECRET_DB"]
    db_url_dict = get_secret(secret_db)
    db_url = db_url_dict["conn_string_sources"]

except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc
logger.info("Obteniendo recursos de S3")


def download_file():
    """
    Funcion encargada de la descarga del archivo desde S3
    :param Bucket_name nombre del busket donde se alojara el archivo
    :param FILE_NAME nombre del archivo
    :param df_tradition información del archivo en dataframe
    """
    FILE_NAME = path.split("/")[-1]
    s3 = boto3.client("s3")
    s3_object = s3.get_object(Bucket=Bucket_name, Key=path)
    input_file = io.BytesIO(s3_object["Body"].read())
    file_size = sys.getsizeof(input_file)
    file_size = int(file_size)
    if file_size > MAX_FILE_SIZE:
        logger.info(
            "El archivo %s es sospechosamente grande, no sera procesado.", FILE_NAME
        )
        raise_msg = f"El archivo {FILE_NAME} supera el tamanio maximo aceptado. "
        raise_msg += "Maximo: {MAX_FILE_SIZE}B. Informado: {file_size}B"
        raise PlataformError(raise_msg)
    
    logger.info('tiene el archivo en .io')
    logger.info(f'Tamanio del archivo {file_size}')
    logger.info("archivo exitoso")
    df_tradition = pd.read_excel(input_file, skiprows=[6])
    logger.info("Nombre del archivo: %s", FILE_NAME)
    logger.debug("DataFrame: ", df_tradition.to_string())
    return df_tradition


class actions_db:
    def create_connection(self, db_url):
        """
        Función encargada de realizar la connexión a la base de datos src
        """
        try:
            sql_engine = sa.create_engine(db_url, connect_args={"connect_timeout": 2})
            db_connection = sql_engine.connect()

            return db_connection
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc


class etl_gfi:
    def __init__(self) -> None:
        self.action_db = actions_db()

    def selected_data(self, df_gfi):
        """
        Función encargada de seleccionar el rango de la data necesaria para
        el análisis de la data
        Ingresa el dataframe que se obtuvo del archivo excel
        :return dataframe informacion necesaria para realizar los calculos
        """
        error_msg = "No se logro identificar la data para limpiar"
        try:

            df_gfi = df_gfi.iloc[5:15, 1:6]
            df_gfi.columns = df_gfi.iloc[0]
            df_gfi = df_gfi.loc[4:]
            df_gfi = df_gfi.drop(df_gfi.index[0])
            logger.debug(
                f"Dataframe de entrada con solo la informacion de inters:\n{df_gfi}"
            )
            return df_gfi
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def clean_data(self, clean_data):
        """
        Función encargada de limpiar la imfomación que proviente del dataframe
        eliminando los parentesis de la columan nodo para liberar el tenor
        :return dataframe con sin paréntesis en la columna tenor
        """
        error_msg = "Se realiza la limpieza de los caracteres especiales del dataframe"
        try:
            clean_data = clean_data.replace(r"""["()]""", "", regex=True)
            return clean_data
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def origin_validation(self, validated_df):
        """
        Función encargada de validar el origen de la data, si es por MEGATRON o por
        valoracion
        de acuerdo al event buscar al ruta del archivo y definira si es un archivo
        que viene del area de valoracion o si es de  MEGATRON
        :param path_s3 path para validar el origen de la informacion
        Despues de validar la informacion agregara una nueva columan indicando el
        origen
        :return dataframe con la informacion del origen de la data
        """
        error_msg = "No se logro validar el origen del archivo"
        try:
            logger.info(
                "Se intenta realizar la validacion del origen de la informacion..."
            )
            if valuation_path in path:
                validated_df["author"] = "VALORACION"
                print("El autor del documento fue el area de valoracion")
            else:
                validated_df["author"] = "MEGATRON"
            print("Se valida correctamente el origen de la infomracion.")
            return validated_df
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def create_columns(self, df_download_new_columns, tenor, broker):
        """
        Función encargada de la creacion de las nuevas columnas  para
        tenor y broker
        :param new_columns parámetro indicador para crear las columnas
        :param dataframe dataframe con las nuevas columnas
        :param broker: el nombre del broker
        :param tenor: los tenores a agregar en la nueva columan
        """
        error_msg = "No se logro crear las columnas de broker y tenor"
        try:
            logger.info("Se intenta crear las nuevas columnas...")
            df_download_new_columns["broker"] = broker
            df_download_new_columns["mid_fwd"] = ""
            df_download_new_columns["Tenor"] = tenor
            df_download_new_columns["Tenor"] = df_download_new_columns[
                "Tenor"
            ].str.upper()
            logger.info("Se crea las nuevas columnas correctamente.")
            return df_download_new_columns
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def drop_columns(self, new_df_gfi):
        """
        Función encargada de eliminar las columnas que no son necesarias para
        ingresar a la base de datos
        return dataframe con la información necesaria para ingresar a
        la base de datos
        """
        error_msg = "No se logro eliminar las columnas correctamente"
        try:
            logger.info("Se intenta eliminar las columnas innecesarias...")
            new_df_gfi = new_df_gfi.drop(
                ["Nodo", "Fecha"],
                axis=1,
            )
            logger.info("Se eliminan las columnas correctamente.")
            return new_df_gfi
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def rename_columns(self, df_rename_columns, new_columns):
        """
        Función encargada de renombrar las columnas
        return dataframe con los nuevos nombres de las columnas
        :param new_columns: los nombres de las nuevas columnas
        """
        logger.info("Se intenta renombrar las columnas...")
        error_msg = "No se logro renombrar las columnas"
        try:
            logger.info("rename_columns")
            logger.info(type(new_columns))
            logger.info(new_columns)
            df_rename_columns.rename(columns=new_columns, inplace=True)
            logger.info("Se renombras las columnas correctamente.")
            return df_rename_columns
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def calculate_mid(self, df_calculate_mid):
        """
        Función encargada de calcular el MID
        return dataframe con la informacion del MID
        """
        error_msg = "No se pudo calcular el mid"
        try:
            logger.info("Se intenta calcular el MID...")
            df_calculate_mid["mid_fwd"] = (
                (df_calculate_mid["bid"] + df_calculate_mid["ask"]) / 2
            ).round(3)
            logger.info("Se realiza el calculo para el MID.")
            return df_calculate_mid
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def validate_null_data(self, df_validate_data, max_null_data):
        """
        Función encargada de validar que en las columnas de Bid y Ask no lleguen
        valores nulos
        :param max_null_data máximo de valores data permitidos

        """
        error_msg = "No se pudo validar el contenido del dataframe"
        try:
            logger.info("valores nulos...")
            null_value = df_validate_data[["bid", "ask"]].isnull().sum().sum()
            logger.info("Se obtiene el numero de valores nulos")
            if null_value <= max_null_data:
                df_validate_data
                return df_validate_data
            else:
                raise UserError("No hay datos en las columnas bid y Ask")
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def add_new_row(self, df_new_row):
        """
        Función encargada de la creacion de la nueva fila para el tenor ON
        :param new_row: nueva fila a agregar en el dataframe
        :return  dataframe con la nueva fila
        """
        error_msg = "No se puede agregar la fila para ON"
        try:
            logger.info("Se intenta crear la fila para ON...")
            new_row = pd.DataFrame(
                {
                    "broker": ["GFI"],
                    "Tenor": ["ON"],
                    "days": [1],
                    "mid_fwd": [0],
                    "bid": [0],
                    "ask": [0],
                    "author": "MEGATRON",
                }
            )
            df_new_row = pd.concat([new_row, df_new_row])
            logger.info("Se crea la fila para ON.")
            return df_new_row
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def set_valuation_date(self, set_valuation_date, file_name, format_date):
        """
        Función encargada de establecer la fecha de valoración segun el nombre del
        archivo
        :param format_date: formato de fecha para la ETL
        :return dataframe con la fecha de valoración
        """
        error_msg = "No se pudo hallar la fecha de valoracion"
        try:
            file_name = file_name.split(".")
            file_name = file_name[0]
            valuation_date = file_name[-6:]
            valuation_date = datetime.strptime(valuation_date, "%d%m%y")
            valuation_date = valuation_date.strftime(format_date)
            set_valuation_date["valuation_date"] = valuation_date
            return set_valuation_date
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def order_df(self, df_val_date, order_df):
        """
        Función encargada de ordenar el dataframe segun criterio de aceptación
        :param order_df: orden indicado para entregar el dataframe
        :return  dataframe organizado
        """
        error_msg = "No se pudo ordenar el la informacion"
        try:
            logger.info("Se intenta organizar el dataframe...")
            df_val_date = df_val_date[order_df]
            logger.info("Se ordenan las columnas del dataframe.")
            return df_val_date
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def insert_data_to_db(self, data_to_insert, db_url, name_table):
        """
        Inserta la información  en la tabla  src_otc_fwd_local_brokers, de acuerdo a la
        informacion que traera el archivo que se descargo en el S3 y  se realizo  la
        limpieza de los datos
        :param data_to_insert data a insertar
        :param db_url string de conexión
        :param name_table  nombre de la tabla
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            data_to_insert = data_to_insert.reset_index()
            db_connection = self.action_db.create_connection(db_url)
            db_connection = db_connection.connect()
            logger.info("Conexion exitosa.")
            try:
                data_to_insert = data_to_insert.sort_values(["days"])
                if data_to_insert.empty:
                    logger.info("No hay informacion")
                else:
                    data_to_insert.to_sql(
                        name=name_table,
                        con=db_connection,
                        if_exists="append",
                        index=False,
                    )
                    db_connection.close()
                    logger.info("Se inserto la informacion de manera exitosa")
                return data_to_insert
            except sa.exc.SQLAlchemyError as sql_exc:
                logger.error(create_log_msg(error_msg))
                raise_msg = (
                    "La base de datos no acepto los datos. validar tipos de datos"
                )
                raise PlataformError(raise_msg) from sql_exc
            except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def disable_previous_info(self, info_df, db_table, db_url):
        """
        Funcion encargada de deshabilitar la informacion anterior en la
        tabla, en caso de que se vuelva a sobreescribir informacion
        :param valuation_date_str Fecha de valoracion a tener en cuenta en
        la consulta
        :param broker: broker a tener en cuenta para la consulta
        :param update_query query para actualizar información
        """
        error_msg = "No se pudo realizar la deshabilidacion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.action_db.create_connection(db_url)
            conn = db_connection.connect()
            logger.info("Conexion exitosa.")
            try:
                info_df.set_index("Tenor", inplace=True)
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

                conn.execute(update_query, query_params)
                conn.close()
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


def run():

    df_tradition = download_file()
    file_name = etl_gfi()
    selected_df = file_name.selected_data(df_tradition)
    clean_df = file_name.clean_data(selected_df)
    df_download_new_columns = file_name.create_columns(clean_df, tenor, broker)
    new_df_gfi = file_name.drop_columns(df_download_new_columns)
    df_rename_columns = file_name.rename_columns(new_df_gfi, new_columns)
    df_calculate_mid = file_name.calculate_mid(df_rename_columns)
    df_validate_data = file_name.validate_null_data(df_calculate_mid, max_null_data)
    df_new_row = file_name.add_new_row(df_validate_data)
    FILE_NAME = path.split("/")[-1]
    df_val_date = file_name.set_valuation_date(df_new_row, FILE_NAME, format_date)
    validated_df = file_name.origin_validation(df_val_date)
    df_to_insert = file_name.order_df(validated_df, order_df)
    logger.debug(df_to_insert)
    file_name.disable_previous_info(df_to_insert, table, db_url)
    data_to_insert = file_name.insert_data_to_db(df_to_insert, db_url, table)
    logger.debug(f"Dataframe insertada en base de datos {data_to_insert}")
    logger.info("Se finaliza la ejecucion del main")


if __name__ == "__main__":
    run()
