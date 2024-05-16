import logging
from datetime import datetime, timedelta, timezone
import boto3
import json

import pandas as pd
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError, UserError
from precia_utils.precia_sftp import connect_to_sftp
from common_library_email_report.ReportEmail import ReportEmail

logger = setup_logging(logging.INFO)


class FileManager:
    MAX_FILE_SIZE = 5e7

    def __init__(self, sftp_credentials, path) -> None:
        self.sftp_credentials = sftp_credentials
        self.path = path
        self.sftp_client = connect_to_sftp(SFPT_CREDENTIALS)
        self.info_df = self.get_data_file_from_sftp()

    def get_data_file_from_sftp(self):
        raise_msg = "Fallo la obtencion del archivo"
        try:
            with self.sftp_client.open_sftp() as sftp_connect:
                filesize = sftp_connect.stat(self.path).st_size
                if filesize > FileManager.MAX_FILE_SIZE:
                    logger.error("El archivo supera el tamaño permitido")
                    raise ValueError("El archivo es demasiado grande")
                logger.info("Comienza la obtencion del archivo series_uvr.csv...")

                sftp_file = sftp_connect.open(self.path)
                info_df = pd.read_csv(
                    sftp_file,
                    sep=",",
                )

            logger.info("Se obtuvo el archivo de uvr correctamente")
            return info_df
        except (Exception,) as file_exc:
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg)


class DbManager:
    """Administra la conexion a BD y provee metodos para realizar consultas SQL"""

    CALENDAR_DB_TABLE = "precia_utils_calendars"
    RATES_DB_TABLE = "src_exchange_rates"
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, conn_url: str, timeout: int) -> None:
        self.conn_url = conn_url
        self.engine = sa.create_engine(
            conn_url, connect_args={"connect_timeout": timeout}
        )

    def disable_and_insert_df(self, data_df):
        """
        Carga el valor de la TRM en la base de datos (un registro por fecha a
        la que aplica). Si ya existen valores para las fechas a cargar,
        desabilita los registros y los carga de nuevo
        """
        error_msg = "No se pudo desabilitar e insertar los datos"
        try:
            logger.info("Ejecutando insert query")
            self.disable_previous_info(data_df, DbManager.RATES_DB_TABLE)
            with self.engine.connect() as conn:
                data_df.to_sql(
                    name=DbManager.RATES_DB_TABLE,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
            logger.info("Insert query ejecutada")
        except sa.exc.SQLAlchemyError as sql_exc:
            logger.error(create_log_msg("No se pudo ejecutar el query"))
            raise PlataformError(error_msg) from sql_exc
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def disable_previous_info(self, data_df: pd.DataFrame, db_table: str):
        """Deshabilita la informacion anterior que coincida con el id precia y la
        fecha de vigencia del dataframe dado

        Args:
            info_df (pd.DataFrame): dataframe de referencia
            db_table (str): tabla en base de datos

        Raises:
            PlataformError: cuando la deshabilitacion de informacion falla
        """
        try:
            logger.info("Construyendo query de actualizacion...")
            update_query = sa.sql.text(
                f"""
                UPDATE {db_table} SET status_info = 0 WHERE effective_date in
                :effective_date_list and id_precia = 'UVRCOP'
                """
            )
            query_params = {
                "effective_date_list": data_df["effective_date"].to_list(),
            }
            logger.info("Query de actualizacion construido con exito")
            logger.info("Ejecutando query de actualizacion...")
            with self.engine.connect() as conn:
                conn.execute(update_query, query_params)
            logger.info("Query de actualizacion ejecutada con exito")
            logger.info("Intentando cerrar la conexion a BD")
            logger.info("Conexion a BD cerrada con exito")
        except (Exception,) as ins_exc:
            raise_msg = "Fallo la deshabilitacion de informacion en BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from ins_exc

    def check_if_day_is_business(self, valuation_date: datetime) -> int:
        """
        Consulta si la fecha dada es un dia habil en el calendario de Colombia

        Args:
            valuation_date (str): Fecha sobre la que se desea realizar la
            consulta

        Returns:
            int:
                1 si la fecha es un dia habil
                0 si no lo es
        """
        try:
            valuation_date_str = valuation_date.strftime(DbManager.DATE_FORMAT)
            select_query = sa.sql.text(
                f"""
                SELECT bvc_calendar FROM {DbManager.CALENDAR_DB_TABLE}
                WHERE dates_calendar = :valuation_date
                """
            )
            query_params = {"valuation_date": valuation_date_str}
            with self.engine.connect() as conn:
                results = conn.execute(select_query, query_params).fetchall()
            return results[0][0]
        except (Exception,) as cdb_exc:
            raise_msg = (
                f"Fallo la consulta de la fecha {valuation_date} en el calendario en BD"
            )
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from cdb_exc

    def get_last_date(
        self,
    ):
        try:
            select_query = sa.sql.text(
                f"""
                SELECT valuation_date FROM {DbManager.RATES_DB_TABLE} where id_precia
                = 'UVRCOP' ORDER BY valuation_date DESC LIMIT 1;
                """
            )
            with self.engine.connect() as conn:
                results = conn.execute(select_query).fetchall()

            if results == []:  # Si no hay valores de UVR en la BD
                last_date_uvr = datetime(1900, 1, 1).date()
            else:
                last_date_uvr = results[0][0]
            return last_date_uvr
        except (Exception,) as cdb_exc:
            raise_msg = "Fallo la consulta de la ultima fecha de UVR BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from cdb_exc

    def get_business_day(self, year: int, month: int, day_number: int) -> datetime:
        try:
            date_regex = f"^{year}-{str(month).zfill(2)}"
            select_query = sa.sql.text(
                f"""
                SELECT dates_calendar FROM {DbManager.CALENDAR_DB_TABLE} WHERE
                dates_calendar REGEXP :date_regex AND bvc_calendar = 1 ORDER BY
                dates_calendar LIMIT :day_number,1;
                """
            )
            query_params = {"date_regex": date_regex, "day_number": day_number - 1}
            logger.debug("query_params:\n%s", query_params)
            with self.engine.connect() as conn:
                results = conn.execute(select_query, query_params).fetchall()

            # Si no hay fechas con el mes y el anio indicados en la BD
            if results == []:
                raise UserError("No hay fechas para el mes y el anio indicados en BD")
            else:
                # conversion de datetime.date a datetime.datetime
                business_day = datetime.combine(results[0][0], datetime.min.time())

            return business_day
        except (Exception,) as cdb_exc:
            raise_msg = "Fallo la consulta del calendario en BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from cdb_exc


class SerieUVRETL:
    DATE_FORMAT = "%d/%m/%Y"

    def __init__(self, input_df: pd.DataFrame, last_bd_date: datetime) -> None:
        self.input_df = input_df
        self.last_bd_datetime = datetime.combine(last_bd_date, datetime.min.time())
        self.cols_dict = {"Date": "valuation_date", "UVR": "value_rates"}
        self.new_cols_dict = {
            "id_precia": "UVRCOP",
            "id_supplier": "PRECIA",
            "effective_date": lambda x: x.valuation_date,
        }
        self.output_df = self.run()

    def translate(self, process_df: pd.DataFrame):
        """Reemplazo de nombres"""
        process_df = process_df.rename(columns=self.cols_dict)
        logger.debug(
            "Ultimas filas del dataframe de uvr despues translate():\n%s",
            process_df.tail(),
        )
        return process_df

    def extract_last_uvr(self, process_df: pd.DataFrame):
        process_df["valuation_date"] = pd.to_datetime(
            process_df["valuation_date"], format="%d/%m/%Y"
        )
        process_df = process_df[process_df["valuation_date"] > self.last_bd_datetime]
        logger.debug(
            "Ultimas filas del dataframe de uvr despues extract_last_uvr():\n%s",
            process_df.tail(),
        )
        return process_df

    def add(self, process_df: pd.DataFrame):
        """Nuevas filas y/o columnas"""
        process_df = process_df.assign(**self.new_cols_dict)
        logger.debug(
            "Ultimas filas del dataframe de uvr despues de add():\n%s",
            process_df.tail(),
        )
        return process_df

    def run(
        self,
    ):
        logger.debug(
            "Ultimas filas del dataframe de uvr crudo sin ningun procesamiento (input_df):\n%s",
            self.input_df.tail(),
        )
        process_df = self.translate(self.input_df)
        process_df = self.extract_last_uvr(process_df)
        process_df = self.add(process_df)
        logger.debug(
            "Ultimas filas del dataframe de uvr despues todo el proceso de la ETL:\n%s",
            process_df.tail(),
        )
        return process_df


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


# Obtencion de parametros de Glue y secretos
try:
    logger.info("Obteniendo parametros de Glue y secretos...")
    PARAMS_LIST = [
        "SMTP_SECRET",
        "DB_SECRET",
        "SFTP_SECRET",
        "RATE_PARITY_LAMBDA",
        "IS_AUTO",
        "BUSINESS_DAY_TO_CONSULT",
    ]
    params_dict = get_params(PARAMS_LIST)

    IS_AUTO = params_dict["IS_AUTO"] == "True"
    LAMBDA_NAME = params_dict["RATE_PARITY_LAMBDA"]
    BUSINESS_DAY_TO_CONSULT = int(params_dict["BUSINESS_DAY_TO_CONSULT"])

    # Secretos
    DB_SECRET = get_secret(params_dict["DB_SECRET"])
    CONN_STRING_UTILS = DB_SECRET["conn_string_aurora"] + DB_SECRET["schema_utils"]
    CONN_STRING_SOURCES = DB_SECRET["conn_string"] + DB_SECRET["schema_src_rates"]
    SMTP_SECRET = get_secret(params_dict["SMTP_SECRET"])
    SMPT_CREDENTIALS = {
        "server": SMTP_SECRET["smtp_server"],
        "port": SMTP_SECRET["smtp_port"],
        "user": SMTP_SECRET["smtp_user"],
        "password": SMTP_SECRET["smtp_password"],
    }
    MAIL_FROM = SMTP_SECRET["mail_from"]
    MAIL_TO = SMTP_SECRET["mail_to"]

    SFTP_SECRET = get_secret(params_dict["SFTP_SECRET"])
    SFPT_CREDENTIALS = {
        "host": SFTP_SECRET["sftp_host"],
        "port": SFTP_SECRET["sftp_port"],
        "username": SFTP_SECRET["sftp_user"],
        "password": SFTP_SECRET["sftp_password"],
    }
    SERIE_UVR_PATH = SFTP_SECRET["serie_uvr_path"]
    logger.debug('SERIE_UVR_PATH: %s',SERIE_UVR_PATH)

    logger.info("Parametros de Glue y secretos obtenidos exitosamente")
except (Exception,) as param_exc:
    raise_msg = "Fallo la obtención de parámetros y secretos"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError(raise_msg) from param_exc


def main():

    error_msg = "Ejecucion de Glue Job interrumpida"
    try:

        # Fecha de hoy para zona horaria Colombia
        colombia_offset = timedelta(hours=-5)
        colombia_timezone = timezone(colombia_offset)
        valuation_date = datetime.now(colombia_timezone)

        logger.info(
            "Fecha actual: %s",
            valuation_date.date(),
        )

        # Objeto para hacer consultas al esquema de BD de sources
        src_db_manager = DbManager(conn_url=CONN_STRING_SOURCES, timeout=3)

        logger.info("Ejecucion automatica: %s", IS_AUTO)
        if IS_AUTO:

            # Fin de semana
            weekend_days = ["saturday", "sunday"]
            weekday = valuation_date.strftime("%A").lower()
            logger.debug("Dia de la semana: %s", weekday)
            if weekday in weekend_days:
                raise_msg = (
                    "Es fin de semana, por lo tanto no se ejecuta la coleccion de UVR"
                )
                logger.error(create_log_msg(raise_msg))
                raise PlataformError(raise_msg)
            # ------------------------------------------------------------------

            # Dia festivo------------------------------------------------------
            utils_db_manager = DbManager(conn_url=CONN_STRING_UTILS, timeout=3)
            is_bussines_date = utils_db_manager.check_if_day_is_business(
                valuation_date=valuation_date
            )
            if is_bussines_date == 0:
                raise_msg = (
                    "No es dia habil, por lo tanto no se ejecuta la coleccion de UVR"
                )
                logger.error(create_log_msg(raise_msg))
                raise PlataformError(raise_msg)
            # ------------------------------------------------------------------

            # Fecha en la que se debe consultar segun metodologia
            business_date_to_consult = utils_db_manager.get_business_day(
                year=valuation_date.year,
                month=valuation_date.month,
                day_number=BUSINESS_DAY_TO_CONSULT,
            )

            # Dia habil antes del dia agendado por metodologia-----------------
            valuation_date_without_tz = valuation_date.replace(tzinfo=None)
            if valuation_date_without_tz < business_date_to_consult:
                raise_msg = "La coleccion de UVR debe ocurrir en o despues del"
                raise_msg += f" {business_date_to_consult} ("
                raise_msg += f"dia habil {BUSINESS_DAY_TO_CONSULT})"
                raise PlataformError(raise_msg)
            # ------------------------------------------------------------------

            # La BD ya esta actualizada----------------------------------------
            last_date_uvr = src_db_manager.get_last_date()
            if valuation_date.month + 1 == last_date_uvr.month:
                raise_msg = "La coleccion de UVR ya se ejecuto este mes ("
                raise_msg += f"{valuation_date.strftime('%B')}) o no se ha "
                raise_msg += "actualizado el archivo 'series_uvr.csv'"
                raise PlataformError(raise_msg)
            # ------------------------------------------------------------------

        file_namager = FileManager(
            sftp_credentials=SFPT_CREDENTIALS, path=SERIE_UVR_PATH
        )

        # fecha mas reciente de tasa uvr cargada e BD
        last_date_uvr = src_db_manager.get_last_date()
        logger.info("Ultima fecha en BD antes de cargar info: %s", last_date_uvr)

        etl = SerieUVRETL(input_df=file_namager.info_df, last_bd_date=last_date_uvr)

        # Si no hay info nueva
        if etl.output_df.empty:
            raise_msg = "No hay informacion en el insumo serie_uvr.csv que no este "
            raise_msg += "ya cargada en base de datos, por lo tanto no se realiza "
            raise_msg += "ninguna insercion en base de datos"
            logger.error(create_log_msg(raise_msg))
            raise UserError(raise_msg)

        src_db_manager.disable_and_insert_df(data_df=etl.output_df)

        last_date_uvr = src_db_manager.get_last_date()
        logger.info("Ultima fecha en BD despues de cargar info: %s", last_date_uvr)

        # Asunto y cuerpo del corrreo
        subject = "Megatron: Ultima fecha con valor para la tasa UVRCOP"
        body = f"""
            Cordial saludo. 

            Se acaba de actualizar los valores de la tasa UVRCOP en la base de datos de tasas de cambio. En la base de datos hay valores hasta: {last_date_uvr}

            Megatron.

            Enviado por el servicio automatico de notificaciones de Precia PPV S.A. en AWS"""

        # Eliminando las tabulaciones de la identacion de python
        body = "\n".join(line.strip() for line in body.splitlines())

        # Enviando correo con ultimas TRMs
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(SMPT_CREDENTIALS)
        message = email.create_mail_base(MAIL_FROM, MAIL_TO)
        email.send_email(smtp_connection, message)

        # Lanzando lambda de paridad de tasas
        new_uvr_df = etl.output_df.astype({"valuation_date": "string"})
        dates_list = new_uvr_df["valuation_date"].to_list()
        dates_list_str = " ".join(dates_list)
        launch_lambda(
            lambda_name=LAMBDA_NAME,
            payload={"valuation_date_list": dates_list_str},
        )

    except PlataformError as pla_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(pla_exc.error_message) from pla_exc
    except (Exception,) as main_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(main_exc.error_message) from main_exc


if __name__ == "__main__":
    main()
