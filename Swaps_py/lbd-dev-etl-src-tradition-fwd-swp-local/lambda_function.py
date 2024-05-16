"""
=============================================================

Nombre:  lbd-dev-etl-src-tradition-fwd-swp-local
Tipo: ETL

Au  tores:
    - Lorena Julieth Torres Hernandez
- Ruben Antonio Parra Medrano
Tecnología - Precia

Ultima modificación: 09/09/2022


ETL desarrollada para obtener un archivo del bucket S3  y realizar la limpieza
de la informacion la cual llegara  dentro del evento, posteriormente realizara
la  descarga   del  archivo  y  su respectivo  analisis,  eliminar  caracteres
prohibidos,  agregar  la informacion  faltante  como  lo es  orden y  precio y
finalmente cargar la informacion en una base de datos


=============================================================
"""

import datetime as dt
from botocore.client import Config
from dateutil import tz
import boto3
import json
import pytz

import pandas as pd
import requests
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_exceptions import UserError, PlataformError
from precia_utils.precia_response import create_error_response
from precia_utils.precia_aws import (
    get_config_secret,
    get_environment_variable,
    get_secret,
)
from ETLutils import connect_to_db

FILE_NAME = "Tradition_Colombia SIN IDENTIFICAR"
BROKER = "TRADITION"
EXPECTED_FILE_COLUMNS = [
    "SISTEMA",
    "TIPO DE DERIVADO",
    "COD CONTRATO",
    "NOMBRE CONTRATO",
    "COD INSTRUMENTO",
    "NOMBRE INSTRUMENTO",
    "SUBYACENTE",
    "FECHA",
    "HORA INICIO",
    "HORA FIN",
    "FECHA EMISION",
    "FECHA DE LIQUIDACION",
    "NOMINAL",
    "PRECIO/TASA",
    "NIVEL BID",
    "NIVEL ASK",
    "MONEDA 1",
    "MONEDA 2",
    "PERIODICIDAD DE PAGO DE LOS CONTRATOS",
    "MONTO",
]
MAX_FILE_SIZE = 5e7
ETL_FORMAT_DATE = "%Y-%m-%d"
SWP_COLUMN_DICT = {
    "derived_type_fwd": "derived_type_swp",
    "market_fwd": "market_swp",
    "underlying_fwd": "underlying_swp",
    "date_fwd": "date_swp",
    "init_hour_fwd": "init_hour_swp",
    "final_hour_fwd": "final_hour_swp",
    "date_issue_fwd": "date_issue_swp",
    "tenor_fwd": "tenor_swp",
    "nominal_fwd": "nominal_swp",
    "order_type_fwd": "order_type_swp",
    "price_fwd": "price_swp",
}
FWD_COLUMN_DICT = {
    "SISTEMA": "systems",
    "TIPO DE DERIVADO": "derived_type_fwd",
    "MERCADO": "market_fwd",
    "SUBYACENTE": "underlying_fwd",
    "FECHA": "date_fwd",
    "HORA INICIO": "init_hour_fwd",
    "HORA FIN": "final_hour_fwd",
    "FECHA EMISION": "date_issue_fwd",
    "TENOR": "tenor_fwd",
    "NOMINAL": "nominal_fwd",
    "ORDEN": "order_type_fwd",
    "PRECIO": "price_fwd",
}

try:
    logger = setup_logging()
    logger.info("[INIT] Inicializando funcion Lambda ...")
    config_secret_name = get_environment_variable("CONFIG_SECRET_NAME")
    CONFIG_SECRET = get_config_secret(config_secret_name)
    VALUATION_DATE = CONFIG_SECRET["test/valuation_date"]
    try:
        dt.datetime.strptime(VALUATION_DATE, ETL_FORMAT_DATE)
    except ValueError:
        VALUATION_DATE = dt.datetime.now().strftime(ETL_FORMAT_DATE)
    logger.info("Fecha de valoracion: %s", VALUATION_DATE)
    secret_region = CONFIG_SECRET["secret/region"]
    API_URL = CONFIG_SECRET["url/api_precia"]
    ENABLED_NOTFICATIONS = get_environment_variable("NOTIFICATION_ENABLED")
    s3_config = Config(connect_timeout=2, retries={"max_attempts": 0})
    s3_client = boto3.client("s3", config=s3_config)
    DATA_LAKE = CONFIG_SECRET["s3/bucket"]
    notification_emails = CONFIG_SECRET["validator/mail_to"]
    RECIPIENTS = notification_emails.replace(" ", "").split(",")
    secret_utils_db_name = CONFIG_SECRET["secret/db_precia_utils"]
    secret_scr_db_name = CONFIG_SECRET["secret/db_src_otc"]
    scr_db_secret = get_secret(secret_region, secret_scr_db_name)
    DB_CONNECTION = connect_to_db(scr_db_secret)
    utils_db_secret = get_secret(secret_region, secret_utils_db_name)
    DB_UTILS_CONNECTION = connect_to_db(utils_db_secret)
    DB_STATUS_TABLE = get_environment_variable("DB_STATUS_TABLE")
    CLOSING_HOUR = get_environment_variable("closing_hour")
    logger.info("[INIT] Conexion exitosa.")
except (Exception,):
    logger.error(create_log_msg("Fallo la inicializacion"))
    FAILED_INIT = True
else:
    FAILED_INIT = False


def lambda_handler(event, context):
    """
    Funcion que hace las veces de main en Lambdas AWS
    :param event: Dict con los datos entregados a la Lambda por desencadenador
    :param context: Objeto con el contexto del Lambda suministrado por AWS
    """
    error_msg = "No se pudo completar el lambda_handler"
    try:
        if FAILED_INIT:
            raise PlataformError("Fallo la inicializacion de la funcion lambda")
        logger.info("Inicia la ejecucion de la funcion ...")
        logger.info("event: %s", json.dumps(event))
        full_s3_file_path, file_size = process_event(event)
        update_status("Ejecutando")
        is_empty_file = validate_file_structure(file_size)
        if is_empty_file:
            update_status("Exitoso")
            body = "Finaliza la ejecucion exitosamente por que el archivo esta vacio"
            send_warning_email(body)
            response = {"statusCode": 200, "body": body}
            logger.info(body)
            return response
        full_tmp_file_path = download_file_from_bucket(full_s3_file_path)
        raw_file_data_df = load_file_data(full_tmp_file_path)
        if raw_file_data_df.empty:
            update_status("Exitoso")
            body = "Finaliza la ejecucion exitosamente por que no encontraron datos en el archivo"
            send_warning_email(body)
            logger.info(body)
            response = {"statusCode": 200, "body": body}
            return response
        logger.info("Inicia la limpieza del archivo previamente descargado...")
        configure_format = configure_df_format(raw_file_data_df)
        
        modified_tenor_df = replace_tenor(configure_format)
        df_tradition = configure_orden(modified_tenor_df)

        tradition_validate_df = validate_column(df_tradition)
        logger.info("dataframe Tradition: \n" + tradition_validate_df.to_string())
        df_to_db = string_replace(tradition_validate_df)
        logger.info("dataframe change Tradition: \n" + df_to_db.to_string())
        
        logger.info("Finaliza la limpieza del archivo.")
        logger.info("Inicia la insercion de la informacion para forwards...")
        data_to_db_fwd(df_to_db)
        
        logger.info("Finaliza la insercion de la informacion para forwards")
        logger.info("Inicia la insercion de la informacion para swaps...")
        data_to_db_swp(df_to_db)
        logger.info("Finaliza la insercion de la informacion para swaps")
        update_status("Exitoso")
        logger.info("Ejecucion de la lambda finalizada.")
        return {"statusCode": 200, "body": "ETL Finalizada exitosamente"}
    except (PlataformError, UserError) as known_exc:
        logger.critical(create_log_msg(error_msg))
        error_response = create_error_response(500, str(known_exc), context)
        report_error(error_response)
        return error_response
    except (Exception,):
        logger.critical(create_log_msg(error_msg))
        msg_to_user = "Error desconocido, solicite soporte"
        error_response = create_error_response(500, msg_to_user, context)
        report_error(error_response)
        return error_response


def configure_df_format(df_download):
    """
    Funcion encargada de configurar el formato de la fecha y de la hora
    :param configure_format Dataframe donde se guardaran las modificaciones de los
    nuevos formatos
    :return: configure_format dataframe con los formatos adecuados
    cambios
    """
    error_msg = "No se pudo realizar la modificacion de los formatos del archivo insumo"
    try:
        logger.info("[configure_df_format] inicia la limpieza...")
        configure_format = pd.DataFrame(df_download)
        configure_format = configure_format.replace(
            r"""["(´"'"" "";")\\*+-]""", "", regex=True
        )
        configure_format.rename(
            columns={
                "Vendor": "Sistema",
                "Trade Date": "Fecha",
                "Market Entry Time": "Hora Inicio",
                "Market Leave Time": "Hora Fin",
                "Amount": "Nominal",
                "NOMBRE CONTRATO": "MERCADO",
                "FECHA DE LIQUIDACION": "TENOR",
            },
            inplace=True,
        )
        closing_hour = get_environment_variable("closing_hour")
        ny_time_today = dt.datetime.now(tz=tz.gettz("America/New_York")).replace(
            tzinfo=None
        )
        bog_time_today = dt.datetime.now(tz=tz.gettz("America/Bogota")).replace(tzinfo=None)
        localFormat = "%H:%M"
        #bog_time_today = bog_time_today.strftime(localFormat)
        #ny_time_today = ny_time_today.strftime(localFormat)
        diff_time = round((ny_time_today - bog_time_today).total_seconds() / (60 * 60), 1)
        logger.info(diff_time)
        logger.info(bog_time_today)
        logger.info(ny_time_today)
        close_market = dt.datetime.strptime(closing_hour, "%H:%M:%S") + dt.timedelta(
            minutes=60 * diff_time
        )
        close_market = close_market.time()
        logger.info(closing_hour)

        
        
        if closing_hour == "14:05:00":
            # TODO 
            final_hour = 100000
        else:
            final_hour = 0
        logger.info(final_hour)
        logger.info(closing_hour)
        """logger.info(
            "Data Antes de configure_format: \n%s", configure_format.to_string()
        )"""
        if "corte11am" in FILE_NAME:
            file_format_date = "%m/%d/%Y"
        else:
            file_format_date = "%d/%m/%Y"
        configure_format["FECHA"] = pd.to_datetime(
            configure_format["FECHA"], format=file_format_date
        )
        configure_format["HORA FIN"] = configure_format["HORA FIN"].fillna(
            str(closing_hour)
        )
        configure_format.loc[
            ~configure_format["HORA FIN"].str.contains(":"), "HORA FIN"
        ] = [
            str(dt.timedelta(days=i))
            for i in configure_format.loc[
                ~configure_format["HORA FIN"].str.contains(":"), "HORA FIN"
            ].values.astype(float)
        ]
        configure_format["HORA INICIO"] = (
            configure_format["HORA INICIO"].str.replace(":", "").astype(int)
        )
        
            
        colombia_timezone = dt.datetime.now(pytz.timezone('America/Bogota')).time()
        newyork_timezone = dt.datetime.now(pytz.timezone('America/New_York')).time()
        hour_extra = 0
        localFormat = "%H:%M"
        colombia_timezone = colombia_timezone.strftime(localFormat)
        newyork_timezone = newyork_timezone.strftime(localFormat)
        if colombia_timezone != newyork_timezone:
            hour_extra = 10000
        logger.debug(colombia_timezone)
        logger.debug(newyork_timezone)
        logger.debug(closing_hour)
        logger.debug(hour_extra)
        configure_format["HORA INICIO"] = configure_format["HORA INICIO"].apply(lambda x: x + hour_extra)
        configure_format["HORA INICIO"] = configure_format["HORA INICIO"].astype(str)
        configure_format["HORA INICIO"] = configure_format["HORA INICIO"].str.zfill(6)
        configure_format["HORA INICIO"] = pd.to_datetime(
            configure_format["HORA INICIO"], format="%H%M%S"
        )
        configure_format["HORA INICIO"] = [
            d.time() for d in configure_format["HORA INICIO"]
        ]
        configure_format["HORA FIN"] = (
            configure_format["HORA FIN"].str.replace(":", "").astype(int)
        )
    
        configure_format["HORA FIN"] = configure_format["HORA FIN"].apply(lambda x: x + hour_extra)
        configure_format["HORA FIN"] = pd.to_numeric(configure_format["HORA FIN"])
        configure_format = configure_format.dropna(subset=["HORA FIN"])
        configure_format["HORA FIN"] = configure_format["HORA FIN"].astype(int)
        configure_format["HORA FIN"] = configure_format["HORA FIN"].astype(str)
        configure_format["HORA FIN"] = configure_format["HORA FIN"].str.zfill(6)
        configure_format["HORA FIN"] = pd.to_datetime(
            configure_format["HORA FIN"], format="%H%M%S"
        )
        configure_format["HORA FIN"] = [d.time() for d in configure_format["HORA FIN"]]
        """logger.info(
            "Data Despues de configure_format: \n%s", configure_format.to_string()
        )
        logger.info("-------------------------------------------------------------------------")
        logger.info(configure_format)"""
        return configure_format
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def replace_tenor(configure_format):
    """
    Funcion encargada de  realizar la modificaciones de los tenores, para esta
    modificacion se tiene en cuenta la columna de subyacente
    :return: modified_tenor_df concatenacion de los anteriores DF posterior a los
    cambios
    """
    error_msg = "No se pudo la modificacion de los tenores"
    try:
        modified_tenor_df = configure_format
        modified_tenor_df.loc[
            modified_tenor_df["TENOR"].str.count("X") > 1, "TENOR"
        ] = modified_tenor_df.loc[
            modified_tenor_df["TENOR"].str.count("X") > 1, "TENOR"
        ].str.replace(
            "X", "V"
        )
        modified_tenor_df["SUBYACENTE"] = modified_tenor_df["SUBYACENTE"].replace(
            {"BASISIBR/SOFR": "USDCO"}, regex=True
        )
        modified_tenor_df["TENOR"] = modified_tenor_df["TENOR"].replace(
            {"12M": "1Y", "1D": "ON"}, regex=True
        )
        return modified_tenor_df
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def configure_orden(modified_tenor_df):
    """
    Funcione encargada de ralizar la validacion del tipo de orden de cada una de
    las puntas y adjuntarlos en una nueva columna identificando los BID y ASK
    param: df_trade dataframe donde d¿se almacenaran las ordenes de trade
    param: modified_tenor_df dataframe donde se realizara las comparaciones de
    las columasn para determinas si son BID o ASK
    :param: df_tradition concatenacion de los dataframe modified_tenor_df y df_trade
    :return: df_tradition
    """
    error_msg = "No se pudo realizar la asignacion para las ordenes de BID y ASK"
    try:
        df_trade = pd.DataFrame(
            modified_tenor_df[
                modified_tenor_df["NIVEL ASK"] == modified_tenor_df["NIVEL BID"]
            ]
        )
        modified_tenor_df.drop(
            modified_tenor_df[
                (modified_tenor_df["NIVEL ASK"] == modified_tenor_df["NIVEL BID"])
            ].index,
            inplace=True,
        )
        df_trade["ORDEN"] = "TRADE"
        df_trade["PRECIO"] = df_trade["NIVEL BID"]
        df_trade["ORDEN-BID"] = df_trade[
            (df_trade["NIVEL BID"] == df_trade["NIVEL ASK"])
        ].index
        df_trade["ORDEN-BID"] = df_trade["ORDEN-BID"].fillna("")
        modified_tenor_df["ORDEN"] = modified_tenor_df["NIVEL BID"].isnull()
        modified_tenor_df["ORDEN"] = modified_tenor_df["ORDEN"].map(
            {True: "ASK", False: "BID"}
        )
        modified_tenor_df["NIVEL ASK"] = modified_tenor_df["NIVEL ASK"].fillna(0)
        modified_tenor_df["NIVEL BID"] = modified_tenor_df["NIVEL BID"].fillna(0)
        modified_tenor_df["PRECIO"] = (
            modified_tenor_df["NIVEL BID"] + modified_tenor_df["NIVEL ASK"]
        )
        modified_tenor_df.loc[
            modified_tenor_df["TIPO DE DERIVADO"] == "U", "NOMINAL"
        ] = (modified_tenor_df["NOMINAL"] * 1000000)
        modified_tenor_df.drop(
            modified_tenor_df[(modified_tenor_df["NOMINAL"] == 0)].index, inplace=True
        )

        
        df_tradition = pd.concat([modified_tenor_df, df_trade])
        df_tradition = df_tradition.drop(
            [
                "COD CONTRATO",
                "COD INSTRUMENTO",
                "NOMBRE INSTRUMENTO",
                "PRECIO/TASA",
                "MONEDA 1",
                "MONEDA 2",
                "PERIODICIDAD DE PAGO DE LOS CONTRATOS",
                "MONTO",
                "NIVEL BID",
                "NIVEL ASK",
            ],
            axis=1,
        )
        logger.debug("validar orden post: \n%s", df_tradition.to_string())
        return df_tradition
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def validate_column(df_tradition):
    """
    Funcion encargada de realizar la valicacion de los BID y ASK
    param: df_to_db: dataframe donde se realiza la validacion y queda listo para
    la insercion en la DB
    return: df_to_db
    """
    error_msg = "No fue posible realizar la validacion de los valores para BID y ASK"
    try:
        df_to_db = df_tradition
        for i in df_to_db["ORDEN-BID"]:
            if i == "":
                raise_msg = "Se encontraron valores de bid y Ask para el mismo registro difrentes"
                raise UserError((raise_msg))
        df_to_db = df_to_db.drop(["ORDEN-BID"], axis=1)
        return df_to_db
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def data_to_db_swp(df_to_db):
    """
    Inserta la informacion  en la tabla  src_otc_options_usdcop, de acuerdo a la
    informacion que traera el archivo que se descargo en el S3 y  se realizo  la
    limpieza de los datos
    :param df_to_swp:  Variable donde  se guardara el dataframe  que se filtrara
    teniendo en cuenta  la columna  de tipo de  derivado y asi mismo realizar la
    insercion en la respectiva tabla
    :param query_delete: Sentencia Mysql para insertar la informacion de acuerdo
    al Dataframe
    :return:  Sentencia Mysql
    """
    error_msg = "No se pudo realizar la insercion de la informacion para swap"
    try:
        logger.debug(df_to_db.to_string())
        df_to_db.rename(columns=SWP_COLUMN_DICT, inplace=True)
        table_swp = get_environment_variable("DB_SWP")
        
        df_to_swp = pd.DataFrame(df_to_db[(df_to_db["derived_type_swp"] == "BASISIBR/SOFR")|(df_to_db["derived_type_swp"] == "SA")])
       
        if df_to_swp.empty:
            logger.info("No hay informacion para swaps")
        else:
            df_to_swp.reset_index(inplace=True, drop=True)
            if "Tradition_Colombia_Report_Trades" in FILE_NAME:
                where_order_type = " AND order_type_swp = 'TRADE'"
            else:
                where_order_type = " AND NOT order_type_swp = 'TRADE'"
            delete_query = f"DELETE FROM {table_swp} WHERE systems = '{BROKER}"
            delete_query += f"' AND date_swp = '{VALUATION_DATE}' {where_order_type}"
            logger.info("delete_query: %s", delete_query)
            DB_CONNECTION.execute(delete_query)
            logger.info("El dataframe  para swaps es:\n" + df_to_swp.to_string())
            df_to_swp.to_sql(
                name=table_swp, con=DB_CONNECTION, if_exists="append", index=False
            )
            df_to_swp_basis = pd.DataFrame(
                df_to_swp[(df_to_swp["derived_type_swp"] == "BASISIBR/SOFR")]
            )
            df_to_swp_basis["derived_type_swp"] = 'SA'
            logger.info(
                "El dataframe  para swaps trade Basis es:\n%s",
                df_to_swp_basis.to_string(),
            )
            
            df_to_swp_basis.to_sql(
                name=table_swp, con=DB_CONNECTION, if_exists="append", index=False
            )
            
                      
            
            logger.info("Se inserto la informacion de manera exitosa")
    except sa.exc.SQLAlchemyError as sql_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = "La base de datos no acepto los datos. validar tipos de datos"
        raise PlataformError(raise_msg) from sql_exc
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def data_to_db_fwd(df_to_db):
    """
    Inserta la informacion  en la tabla  src_otc_options_usdcop, de acuerdo a la
    informacion que traera el archivo que se descargo en el S3 y  se realizo  la
    limpieza de los datos
    :param df_to_fwd:  Variable donde  se guardara el dataframe  que se filtrara
    teniendo en cuenta  la columna  de tipo de  derivado y asi mismo realizar la
    insercion en la respectiva tabla
    :param query_delete: Sentencia Mysql para insertar la informacion de acuerdo
    al Dataframe
    :return:  Sentencia Mysql
    """
    error_msg = "No se pudo realizar la insercion de la informacion para forwards"
    try:
        logger.debug("inicia fwd :\n%s", df_to_db.to_string())
        df_to_db.rename(columns=FWD_COLUMN_DICT, inplace=True)
        table_fwd = get_environment_variable("DB_FWD")
        logger.debug("inicia fwd :\n%s", df_to_db.to_string())
        df_to_fwd = pd.DataFrame(df_to_db[df_to_db["derived_type_fwd"] == "FD"])
        df_to_fwd.drop(df_to_fwd[df_to_fwd["market_fwd"] != 'LOCAL'].index, inplace=True)
        df_to_fwd['tenor_fwd'] = df_to_fwd['tenor_fwd'].replace( {'1W': 'SW'}, regex=True)
        if df_to_fwd.empty:
            logger.info("No hay informacion para forwads")
        else:
            df_to_fwd.reset_index(inplace=True, drop=True)
            if "Tradition_Colombia_Report_Trades" in FILE_NAME:
                where_order_type = " AND order_type_fwd = 'TRADE'"
            else:
                where_order_type = " AND NOT order_type_fwd = 'TRADE'"
            delete_query = f"DELETE FROM {table_fwd} WHERE systems = '{BROKER}"
            delete_query += f"' AND date_fwd = ' {VALUATION_DATE}' {where_order_type}"
            logger.info("delete_query: %s", delete_query)
            DB_CONNECTION.execute(delete_query)
            logger.debug("El dataframe  para forwards es: \n%s", df_to_fwd.to_string())
            df_to_fwd.to_sql(
                name=table_fwd, con=DB_CONNECTION, if_exists="append", index=False
            )
            logger.info("Se inserto la informacion de manera exitosa")
    except sa.exc.SQLAlchemyError as sql_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = "La base de datos no acepto los datos. validar tipos de datos"
        raise PlataformError(raise_msg) from sql_exc
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def process_event(event_dict: dict) -> tuple:
    """
    Procesa el event dado por el trigger S3

    Args:
        event_dict (dict): event dado por el trigger S3

    Raises:
        PlataformError: El event del trigger no tiene el formato o los datos esperados

    Returns:
        tuple: ruta del archivo en S3, tamanio del archivo
    """
    error_msg = "El event del trigger no tiene el formato o los datos esperados"
    try:
        logger.info("Procesando y validando event ... ")
        file_path_s3 = event_dict["Records"][0]["s3"]["object"]["key"]
        logger.info("Ruta del archivo en S3: %s", file_path_s3)
        global FILE_NAME
        FILE_NAME = file_path_s3.split("/")[-1]
        logger.info("Nombre del archivo: %s", FILE_NAME)
        logger.info("Broker: %s", BROKER)
        s3_bucket = event_dict["Records"][0]["s3"]["bucket"]["name"]
        if s3_bucket != DATA_LAKE:
            raise_msg = "Esta funcion Lambda no fue lanzada desde un bucket conocido,"
            raise_msg += f" se ignora la solicitud. Esperado: {DATA_LAKE}. Informado: {s3_bucket}"
            raise PlataformError(raise_msg)
        logger.info("Bucket S3: %s", s3_bucket)
        file_size_s3 = event_dict["Records"][0]["s3"]["object"]["size"]
        logger.info("Tamanio del archivo: %s", file_size_s3)
        logger.info("Event procesado y validado exitosamente.")
        return file_path_s3, file_size_s3
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise
    except KeyError as key_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError("El event no tiene la estructura esperada") from key_exc
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def update_status(status):
    """
    Actualiza la tabla DB_STATUS_TABLE para informar la maquina de estados del proceso
    sobre status actual de la ETL y por ende del insumo en la base de datos SRC.
    Los status validos son: Ejecutando' cuando se lanza el ETL, 'Exitoso' para cuando
    concluya exitosamente, y 'Fallido' para cuando el archivo no sea procesable.
    :param String que define el status actual de la ETL
    """
    error_msg = "No fue posible actualizar el estado de la ETL en DB"
    try:
        schedule_name = FILE_NAME[:-13]
        status_register = {
            "id_byproduct": "swp_local",
            "name_schedule": schedule_name,
            "type_schedule": "ETL",
            "state_schedule": status,
            "details_schedule": VALUATION_DATE,
        }
        logger.info("Actualizando el status de la ETL a: %s", status)
        status_df = pd.DataFrame([status_register])
        status_df.to_sql(
            name=DB_STATUS_TABLE,
            con=DB_UTILS_CONNECTION,
            if_exists="append",
            index=False,
        )
        logger.info("Status actualizado en base de datos.")
    except (Exception,) as status_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = "El ETL no puede actualizar su estatus, el proceso no sera lanzado"
        raise PlataformError(raise_msg) from status_exc


def validate_file_structure(file_size: str) -> bool:
    """
    valida el archivo desde el peso del mismo

    Args:
        file_size (str): Tamanio del archivo insumo

    Raises:
        PlataformError: No supera la validacion de estructura

    Returns:
        bool: True si el archivo esta vacio
    """
    error_msg = "El archivo insumo no supero la validacion de estructura"
    empty_file = False
    try:
        logger.info("Validando la estructura del archivo ... ")
        file_size_int = int(file_size)
        if file_size_int > MAX_FILE_SIZE:
            logger.info(
                "El archivo %s es sospechosamente grande, no sera procesado.", FILE_NAME
            )
            raise_msg = f"El archivo {FILE_NAME} supera el tamanio maximo aceptado. "
            raise_msg += "Maximo: {MAX_FILE_SIZE}B. Informado: {file_size}B"
            raise PlataformError(raise_msg)
        if file_size == 0:
            logger.info("El archivo %s esta vacio.", FILE_NAME)
            empty_file = True
        logger.info("Validacion finalizada.")
        return empty_file
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def send_warning_email(warm_msg: str):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    de una advertencia.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de advertencia"
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info("Notificaciones desabilitadas en las variables de entorno")
            logger.info("No se enviara ningun correo electronico")
            return None
        logger.info("Creando email de advertencia ... ")
        url_notification = f"{API_URL}/utils/notifications/mail"
        logger.info("URL API de notificacion: %s", url_notification)
        subject = (
            f"Optimus-K: Advertencia durante el procesamiento del archivo {FILE_NAME}"
        )
        body = f"""

        Cordial Saludo.

        Durante el procesamiento del archivo insumo {FILE_NAME} del broker {BROKER} \
        se presento una advertencia que NO detiene el proceso.

        Advertencia informada:

        {warm_msg}


        Optimus K.
        """
        body = body.replace("        ", "")
        payload = {"subject": subject, "recipients": RECIPIENTS, "body": body}
        logger.info("Email creado")
        logger.info("Enviando email de error ... ")
        mail_response = requests.post(url_notification, json=payload, timeout=2)
        api_mail_code = str(mail_response.status_code)
        api_mail_text = mail_response.text
        logger.info(
            "Respuesta del servicio de notificaciones: code: %s, body: %s",
            api_mail_code,
            api_mail_text,
        )
        if mail_response.status_code != 200:
            raise_msg = (
                "El API de notificaciones por email no respondio satisfactoriamente."
            )
            raise PlataformError(raise_msg)
        logger.info("Email enviado.")
        return None
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def download_file_from_bucket(full_bucket_path: str) -> str:
    """
    Descarga el archivo informado por event del trigger desde S3 AWS

    Args:
        full_bucket_path (str): Ruta del archivo en el bucket S3

    Raises:
        PlataformError: Cuando no es posible descargar el archivo en S3

    Returns:
        str: Ruta donde se descargo el archivo
    """
    error_msg = "No se pudo descargar el archivo del bucket s3"
    try:
        logger.info("Descargando el archivo %s de %s ... ", full_bucket_path, DATA_LAKE)
        full_lambda_path = "/tmp/" + FILE_NAME
        s3_client.download_file(DATA_LAKE, full_bucket_path, full_lambda_path)
        logger.info("Archivo descargado como %s", full_lambda_path)
        return full_lambda_path
    except (Exception,) as down_exc:
        logger.error(create_log_msg(error_msg))
        raise_msg = (
            f"El servicio S3 AWS nego la descarga del archivo {full_bucket_path}"
        )
        raise PlataformError(raise_msg) from down_exc


def load_file_data(lambda_file_path: str) -> pd.DataFrame:
    """
    Carga los datos del archivo insumo en un DataFrame Pandas

    Args:
        lambda_file_path (str): Ruta del archivo insumo en la lambda

    Raises:
        UserError: Cuando el archivo insumo no tiene los datos o formato esperado

    Returns:
        pd.DataFrame: DataFrame con los datos para la base de datos
    """
    error_msg = "No se pudo transformar la informacion"
    try:
        logger.info("Extrayendo los datos del archivo como dataframe Pandas ...")
        file_extension = FILE_NAME.split(".")[-1].lower()
        if file_extension == "csv":
            data_df = pd.read_csv(lambda_file_path, sep=",")
            if data_df.shape[1] < len(EXPECTED_FILE_COLUMNS):
                data_df = pd.read_csv(lambda_file_path, sep=";")
        else:
            raise UserError(f"El archivo {FILE_NAME} no puede procesarse con este ETL.")
        logger.info("Extraccion de los datos exitosa.")
        logger.info(
            "Datos orginales del archivo. data_df(5):\n%s", data_df.head().to_string()
        )
        data_df.dropna(axis=0, how="all", inplace=True)
        data_columns = list(data_df.columns)
        if set(data_columns) != set(EXPECTED_FILE_COLUMNS):
            raise_msg = "El archivo no tiene las columnas esperadas."
            raise_msg += (
                f" Esperadas:{EXPECTED_FILE_COLUMNS}. Encontradas: {data_columns}"
            )
            raise UserError(raise_msg)
        if data_df.empty:
            logger.warning("El DataFrame de los datos del archivo esta vacio.")
            return pd.DataFrame()
        if not (data_df["FECHA"] == data_df["FECHA"].iloc[0]).all():
            raise_msg = "La columna 'FECHA' No tiene el mismo valor en todas las filas."
            raise UserError(raise_msg)
        if not (data_df["SISTEMA"] == data_df["SISTEMA"].iloc[0]).all():
            warn_msg = (
                "La columna 'SISTEMA' No tiene el mismo valor en todas las filas."
            )
            logger.warning(warn_msg)
        data_broker = data_df["SISTEMA"].iloc[0]
        if data_broker != BROKER:
            log_msg = "El broker del archivo no coincide con el esperado. "
            log_msg += f"Esperado: {BROKER}. Encontrado: {data_broker}"
            logger.warning(log_msg)
        file_valuation_date = data_df["FECHA"].iloc[0]
        if "corte11am" in FILE_NAME:
            format_date = "%m/%d/%Y"
        else:
            format_date = "%d/%m/%Y"
        file_valuation_date = dt.datetime.strptime(
            file_valuation_date, format_date
        )
        data_valuation_date = file_valuation_date.strftime(ETL_FORMAT_DATE)
        global VALUATION_DATE
        if data_valuation_date != VALUATION_DATE:
            log_msg = (
                "La fecha de valoracion del archivo no coincide con la configurada. "
            )
            log_msg += (
                "Si esta en pruebas, ignore esta advertencia, en otro caso solicite "
            )
            log_msg += f"soporte. Configurada: {VALUATION_DATE}. Encontrada: {data_valuation_date}"
            logger.warning(log_msg)
            send_warning_email(log_msg)
            VALUATION_DATE = data_valuation_date
        logger.info("Carga de los datos del archivo exitosa.")
        return data_df
    except UserError:
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def report_error(error_response: dict):
    """
    Ejecuta todas las funciones que permiten notificar el error al ejecutar el ETL

    Args:
        error_response (dict): Resume el error de la ejecucion con el stack_trace
    """
    try:
        logger.info("Notificando el error ...")
        update_status("Fallido")
    except (Exception,):
        logger.error(create_log_msg("No se pudo actualizar el status: Fallido"))
    try:
        send_error_email(error_response)
        logger.info("Error notificado")
    except (Exception,):
        log_msg = "No se pudo enviar el correo de notificacion de error"
        logger.error(create_log_msg(log_msg))


def send_error_email(error_response: dict):
    """
    Solicita al endpoint '/utils/notifications/mail' de la API interna que envie un correo en caso
    que el ETL no finalice su ejecucion correctamente.
    :param recipients, lista de los correos electronicos donde llegara la notificacion de error
    :param error_response, diccionario con el mensaje de error que se quiere enviar
    """
    error_msg = "No se pudo enviar el correo con la notificacion de error"
    try:
        if not eval(ENABLED_NOTFICATIONS):
            logger.info("Notificaciones desabilitadas en las variables de entorno")
            logger.info("No se enviara ningun correo electronico")
            return None
        logger.info("Creando email de error ... ")
        url_notification = f"{API_URL}/utils/notifications/mail"
        logger.info("URL API de notificacion: %s", url_notification)
        error_msg = json.dumps(error_response, skipkeys=True, allow_nan=True, indent=6)
        subject = f"Optimus-K: Fallo el procesamiento del archivo {FILE_NAME}"
        body = f"""

        Cordial Saludo.

        Durante el procesamiento del archivo insumo {FILE_NAME} del broker {BROKER} \
        se presento un error que detiene el proceso.

        Para conocer mas detalles de lo ocurrido, por favor revisar el log referenciado \
        en este mensaje de error:

        {error_msg}

        Con los valores 'log_group', 'log_stream' y 'request_id' puede ubicar el log en \
        el servicio ColudWatch AWS.

        Optimus K.
        """
        body = body.replace("        ", "")
        payload = {"subject": subject, "recipients": RECIPIENTS, "body": body}
        logger.info("Email creado")
        logger.info("Enviando email de error ... ")
        mail_response = requests.post(url_notification, json=payload, timeout=2)
        api_mail_code = str(mail_response.status_code)
        api_mail_text = mail_response.text
        logger.info(
            "Respuesta del servicio de notificaciones: code: %s, body: %s",
            api_mail_code,
            api_mail_text,
        )
        if mail_response.status_code != 200:
            raise_msg = (
                "El API de notificaciones por email no respondio satisfactoriamente."
            )
            raise PlataformError(raise_msg)
        logger.info("Email enviado.")
        return None
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise


def string_replace(df_tradition):
    error_msg = 'No se logro realizar el cambio de los para IBRUVR'
    try:
        logger.info("Dataframe sin mods: \n" + df_tradition.to_string())
        df_tradition_ibrubr = df_tradition.replace(["UVRIBR","UVR/IBR"], "IBRUVR", regex=True)
        logger.info("Dataframe mods uvr: \n" + df_tradition_ibrubr.to_string())
        df_tradition_fin = df_tradition_ibrubr.replace(["BASIS IBR/SOFR"], "USDCO", regex=True)
        return df_tradition_fin
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise