from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import logging
import boto3
import json

import pandas as pd
import requests
import sqlalchemy as sql

from precia_utils.precia_exceptions import PlataformError, UserError
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from common_library_email_report.ReportEmail import ReportEmail

logger = setup_logging(logging.INFO)

FORMAT_DATE = "%Y-%m-%d"
IDENTIFIER = "TRM"
RATES_DB_TABLE = "src_exchange_rates"
API_TIMEOUT = 12
DB_CONN_TIMEOUT = 3


class SfcApiConsumer:
    """
    Consulta la TRM al web service de la superfinanciera de Colombia y crea un
    dataframe listo para cargar el la BD de tasas de cambio
    """

    def __init__(self, api_url: str, valuation_date: datetime, timeout) -> None:
        self.api_url = api_url
        self.valuation_date = valuation_date
        self.timeout = timeout
        self.query_date_str = (valuation_date + timedelta(days=1)).strftime(FORMAT_DATE)
        self.api_response_text = self.request_web_service_sfc()
        self.trm_df, self.dates_list = self.extract_trm()

    def request_web_service_sfc(self):
        """
        Consulta la TRM vigente consumiendo el web service de la superfinanciera
        para la fecha dada
        """
        error_msg = "Fallo la solicitud a la web api."
        try:
            headers = {"Content-Type": "application/xml"}
            body = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:act="http://action.trm.services.generic.action.superfinanciera.nexura.sc.com.co/">
                    <soapenv:Header/>
                    <soapenv:Body>
                        <act:queryTCRM>
                            <tcrmQueryAssociatedDate>{self.query_date_str}</tcrmQueryAssociatedDate>
                        </act:queryTCRM>
                    </soapenv:Body>
                </soapenv:Envelope>
                """
            trm_null_response = (
                "No se ha encontrado el valor para la TRM en la fecha dada"
            )
            logger.info("Haciendo solicitud post a la Web API")
            api_response = requests.post(
                self.api_url, data=body, headers=headers, timeout=self.timeout
            )
            logger.info("Respuesta de la Web API obtenida")
            api_response_code = api_response.status_code
            if api_response_code != 200:
                raise PlataformError("El API no respondio satisfactoriamente.")
            api_response_text = api_response.text
            logger.debug("Respuesta de la API:\n%s", api_response_text)
            if trm_null_response in api_response_text:
                raise PlataformError(trm_null_response)
            return api_response_text
        except requests.exceptions.Timeout as time_exc:
            logger.error(create_log_msg("Tiempo de espera agotado"))
            raise PlataformError(error_msg) from time_exc
        except PlataformError:
            logger.error(create_log_msg(error_msg))
            raise
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def extract_trm(self) -> tuple:
        """
        Transforma la respuesta del web service de la superfinanciera en un
        dataframe
        """
        error_msg = "No se pudieron extraer los datos. {}"
        try:
            root_data = ET.fromstring(self.api_response_text)
            trm_data_xml = root_data[0][0][0]
            trm_data_dict = dict()
            for item in trm_data_xml:
                trm_data_dict[item.tag] = item.text
            logger.debug("XML convertido en diccionario: %s", trm_data_dict)
            initial_validity_date = datetime.strptime(
                trm_data_dict["validityFrom"][:-15], FORMAT_DATE
            )
            logger.info("initial_validity_date: %s", initial_validity_date)
            final_validity_date = datetime.strptime(
                trm_data_dict["validityTo"][:-15], FORMAT_DATE
            )
            logger.info("final_validity_date: %s", final_validity_date)
            delta_validity_date = (final_validity_date - initial_validity_date).days
            data_list = []
            dates_list = []
            for days in range(delta_validity_date + 1):
                iteration_valuation_date = self.valuation_date + timedelta(days=days)
                iteration_valuation_date_str = datetime.strftime(
                    iteration_valuation_date, FORMAT_DATE
                )
                daily_trm = {
                    "id_supplier": "SFC",
                    "effective_date": final_validity_date.strftime(FORMAT_DATE),
                    "valuation_date": iteration_valuation_date,
                    "value_rates": float(trm_data_dict["value"]),
                    "id_precia": "USDCOP",
                }
                data_list.append(daily_trm)
                dates_list.append(iteration_valuation_date_str)
            trm_df = pd.DataFrame(data_list)
            return trm_df, dates_list
        except KeyError as key_exc:
            error_msg = error_msg.format("Modificacion fallida")
            logger.error(create_log_msg(error_msg))
            raise_msg = (
                "No se pudo realizar la modificacion de los formatos del archivo."
            )
            raise PlataformError(raise_msg) from key_exc
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise


class DbManager:
    """Administra la conexion a BD y provee metodos para realizar consultas SQL"""

    CALENDAR_DB_TABLE = "precia_utils_calendars"

    def __init__(self, conn_url: str, timeout: int) -> None:
        self.conn_url = conn_url
        self.engine = sql.create_engine(
            conn_url, connect_args={"connect_timeout": timeout}
        )

    def disable_and_insert_df(self, data_df, db_table):
        """
        Carga el valor de la TRM en la base de datos (un registro por fecha a
        la que aplica). Si ya existen valores para las fechas a cargar,
        desabilita los registros y los carga de nuevo
        """
        error_msg = "No se pudo desabilitar e insertar los datos"
        try:
            logger.debug("Datos para cargar en la BD:\n%s", data_df.to_string())
            logger.info("Ejecutando insert query")
            self.disable_previous_info(data_df, db_table)
            with self.engine.connect() as conn:
                data_df.to_sql(name=db_table, con=conn, if_exists="append", index=False)
            logger.info("Insert query ejecutada")
        except sql.exc.SQLAlchemyError as sql_exc:
            logger.error(create_log_msg("No se pudo ejecutar el query"))
            raise PlataformError(error_msg) from sql_exc
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise

    def update_status(self, status, db_table):
        """
        Actualiza la tabla DB_STATUS_TABLE para informar la maquina de estados del proceso
        sobre status actual de la ETL y por ende del insumo en la base de datos SRC.
        Los status validos son: Ejecutando' cuando se lanza el ETL, 'Exitoso' para cuando
        concluya exitosamente, y 'Fallido' para cuando el archivo no sea procesable.
        :param String que define el status actual de la ETL
        """
        error_msg = "No fue posible actualizar el status."
        try:
            status_register = {
                "id_byproduct": "fwd_local",
                "name_schedule": "sfc_trm",
                "type_schedule": "ETL",
                "state_schedule": status,
                "details_schedule": valuation_date,
            }
            logger.info(status_register)
            logger.info("Actualizando el status de la ETL a: %s", status)
            status_df = pd.DataFrame([status_register])
            with self.engine.connect() as conn:
                status_df.to_sql(
                    name=db_table,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
            logger.info("Status actualizado en base de datos.")
        except sql.exc.SQLAlchemyError as sql_exc:
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
            id_precia = data_df.iloc[0]["id_precia"]
            effective_date = data_df.iloc[0]["effective_date"]
            update_query = sql.sql.text(
                f"""UPDATE {db_table}
                    SET status_info= :status_info WHERE effective_date = :effective_date 
                    and id_precia = :id_precia
                    """
            )
            query_params = {
                "status_info": 0,
                "effective_date": effective_date,
                "id_precia": id_precia,
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
            raise PlataformError(raise_msg) from ins_exc

    def get_trm_of_last_days(self, valuation_date, days, db_table):
        """
        Obtiene el valor de la tasa representativa del mercado colombiano (TRM
        o USDCOP) para los dias mas recientes anteriores a la fecha de consulta
        indicada

        Args:
            df_to_match_with_calendar (_type_): _description_
            date_valuation (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            dates_list = [
                datetime.strftime(valuation_date - timedelta(days=day), "%Y-%m-%d")
                for day in range(0, days)
            ]
            select_query = sql.sql.text(
                f"""
                SELECT value_rates,valuation_date,effective_date FROM {db_table}
                WHERE valuation_date in :dates_list AND status_info = 1 AND
                id_precia = 'USDCOP'
                """
            )
            query_params = {"dates_list": dates_list}
            with self.engine.connect() as conn:
                last_trms_df = pd.read_sql(select_query, conn, params=query_params)
            return last_trms_df
        except (Exception,) as tld_exc:
            raise_msg = f"Fallo la consulta de la trm para los ultimos {days} dias"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from tld_exc

    def check_if_day_is_business(self, valuation_date: datetime) -> int:
        """
        Consulta si la fecha dada es un dia habil en el calendario de Colombia

        Args:
            valuation_date (str): Fecha sobre la que se desea realizar la
            consulta

        Returns:
            int: 1 o 0. 1 si la fecha es un dia habil y 0 si no lo es
        """
        try:
            valuation_date_str = valuation_date.strftime("%Y-%m-%d")
            select_query = sql.sql.text(
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
            raise_msg = "Fallo la consulta del calendario en BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError(raise_msg) from cdb_exc


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


try:
    logger.info("Obteniendo parametros de Glue y secretos...")
    PARAMS_LIST = [
        "VALUATION_DATE",
        "SMTP_SECRET",
        "DB_SECRET",
        "URL",
        "RATE_PARITY_LAMBDA",
    ]
    params_dict = get_params(PARAMS_LIST)

    VALUATION_DATE_STR = params_dict["VALUATION_DATE"]
    SFC_API_URL = params_dict["URL"]
    LAMBDA_NAME = params_dict["RATE_PARITY_LAMBDA"]
    if VALUATION_DATE_STR == "TODAY":
        colombia_offset = timedelta(hours=-5)
        colombia_timezone = timezone(colombia_offset)
        valuation_date = datetime.now(colombia_timezone)
    else:
        valuation_date = datetime.strptime(VALUATION_DATE_STR, FORMAT_DATE)
    logger.info(
        "Fecha sobre la que se desea realizar la consulta: %s", valuation_date.date()
    )
    
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

    logger.info("Parametros de Glue y secretos obtenidos exitosamente")
except (Exception,) as param_exc:
    raise_msg = "Fallo la obtención de parámetros y secretos"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError(raise_msg) from param_exc


def main():
    error_msg = "Glue Job interrumpido"
    try:
        utils_db_manager = DbManager(
            conn_url=CONN_STRING_UTILS, timeout=DB_CONN_TIMEOUT
        )
        logger.info("Inicia la ejecucion del Glue ...")
        is_business_day = utils_db_manager.check_if_day_is_business(
            valuation_date=valuation_date
        )

        # Si no es dia habil detiene la ejecucion de la lambda
        if is_business_day != 1:
            raise UserError(
                f"La fecha  sobre la que se desea realizar la consulta ({valuation_date.date()}) no es un dia habil"
            )

        # Consultando la TRM del web services de la SFC
        logger.info("Solicitando la TRM (USDCOP) al web service de la SFC...")
        sfc_api_consumer = SfcApiConsumer(
            api_url=SFC_API_URL, valuation_date=valuation_date, timeout=API_TIMEOUT
        )
        logger.info("TRM obtenida")

        # Cargando valor/es de la TRM
        logger.info(
            "Desabilitando informacion anterior en BD e insertando nueva informacio con valor/es de TRM"
        )
        sources_db_manager = DbManager(
            conn_url=CONN_STRING_SOURCES, timeout=DB_CONN_TIMEOUT
        )
        sources_db_manager.disable_and_insert_df(
            data_df=sfc_api_consumer.trm_df, db_table=RATES_DB_TABLE
        )
        logger.info("BD actualizada!")

        # Consultando la TRM de los ultimos 8 dias en BD
        last_trms_df = sources_db_manager.get_trm_of_last_days(
            valuation_date=valuation_date, days=8, db_table=RATES_DB_TABLE
        )
        logger.debug("last_trms_df:\n%s", last_trms_df)
        to_mail_dict = {
            "value_rates": "USDCOP",
            "valuation_date": "Fecha valoracion",
            "effective_date": "Fecha vigencia",
        }
        last_trms_df = last_trms_df.rename(columns=to_mail_dict)

        # Preparando cuerpo de correo con ultimas TRMs
        today_trm = sfc_api_consumer.trm_df.at[0, "value_rates"]
        effective_today = sfc_api_consumer.trm_df.at[0, "effective_date"]
        subject = "Megatron: Valor de la TRM (USDCOP) de los ultimos 8 dias"
        body = f"""
            Cordial saludo. 

            El valor de la tasa de cambio USDCOP que se consulto ({valuation_date.date()}) es de {today_trm} con una fecha de vigencia del {effective_today}

            Y el valor de esta tasa de los últimos 8 dias es:

            {last_trms_df.to_string(index=False)}

            El cargue de información ha sido exitoso

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
        # TODO: Descomentar cuando se cree la lambda de paridad el rol de glue
        dates_list_str = " ".join(sfc_api_consumer.dates_list)
        logger.info("Fechas cargadas en BD: %s", dates_list_str)
        launch_lambda(
            lambda_name=LAMBDA_NAME,
            payload={"valuation_date_list": dates_list_str},
        )

    except PlataformError as pla_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(pla_exc.error_message) from pla_exc
    except (Exception,) as glu_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(glu_exc.error_message) from glu_exc


if __name__ == "__main__":
    main()
