from datetime import datetime
import logging

import boto3
import pandas as pd
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_sftp import connect_to_sftp
from common_library_email_report.ReportEmail import ReportEmail

logger = setup_logging(logging.INFO)
subject = "Megatron - Error: General Calendar Days"
body = "El archivo GeneralCalendarsDay tiene una fecha distinta a la de hoy"
MAX_FILE_SIZE = 5e7

class FileGeneralCalendarDays():
    """Representa el archivo General Calendar que se obtiene en el SFTP
    """

    def get_data_file_from_sftp(self, sftp_connect, route_calendar):
        """Lee el archivo de General Calendar que se encuentra en el SFTP

        Parameters:
        ----------
        sftp_connection: SFTPClient
            Es la conexión al SFTP
        route_calendar: str
            Ruta del archivo de General Calendar Day

        Return:
        Dataframe:
            Contiene los datos de General calendar
        """
        tamanio_archivo = sftp_connect.stat(route_calendar).st_size
        if tamanio_archivo > MAX_FILE_SIZE:
            logger.error("El archivo supera el tamaño permitido")
            raise ValueError('El archivo es demasiado grande')
        logger.info("Comienza la obtención del archivo General Calendar...")
        try:
            archivo_sftp = sftp_connect.open(route_calendar)
            df_calendar_days = pd.read_csv(archivo_sftp, 
            names=['valuation_date', 'calendar', 'frequency', 'name_calendar', 'tenor', 'days' ], 
            sep="\t",
            header=0)
        except(Exception, ):
            logger.error("Fallo la obtención del archivo")

        logger.info("Se obtuvo el archivo General Calendar correctamente")
        return df_calendar_days


class DataBase():
    def __init__(self, url_connection) -> None:
        self.url_connection = url_connection
        self.engine = sa.create_engine(self.url_connection)
    

    def insert_data(self, df_tradition):
        """Inserta la información tratada por la ETL a la base de datos

        Parameters:
        ----------
        df_tradition: DataFrame, required
            Contiene los datos del archivo que se genero
            
        name_table: str, required
            Nombre de la tabla para insertar la información
        """
        try:
            logger.info("Comienza la inserción a la base de datos")
            connection = self.engine.connect()
            df_tradition.to_sql("precia_utils_general_calendars_days", con=connection, if_exists='append', index=False)
            logger.info("Se inserto la información correctamente")
        except(Exception, ):
            logger.error(create_log_msg("Falló la inserción de la información en la base de datos"))
            raise PlataformError("Hubo un error en la inserción de información")
        finally:
            connection.close()
    

    def disable_previous_info(self, df_tradition):
        """Actualiza el status de la información se encuentra en la base de datos
        Parameters:
        -----------
        df_tradition: DataFrame, required
            Contiene los datos a insertar a la base de datos
        
        table_db: str, required
            Nombre de la tabla en base de datos
        """
        valuation_date_str = df_tradition.at[0,'valuation_date']
        logger.info(valuation_date_str)
        logger.info("Se comienza a actualizar el estado del información existente")
        try:
            update_query = sa.sql.text("""UPDATE precia_utils_general_calendars_days
            SET status_info= :status_info  
            and valuation_date = :valuation_date
            """)
            query_params = {
                'status_info': 0,
                'valuation_date': valuation_date_str
            }
            connection = self.engine.connect()
            connection.execute(update_query,query_params)
            connection.close()
            logger.info("Se actualizó el status a los registros correctamente")
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if connection != None:
                connection.close()
            raise PlataformError() from fpc_exc
    

def main():
    params_key = [
        "DATANFS_SECRET",
        "SMTP_SECRET",
        "DB_SECRET",
        "VALUATION_DATE"
    ]
    params_glue = get_params(params_key)
    secret_sftp = params_glue["DATANFS_SECRET"]

    key_secret_sftp = get_secret(secret_sftp)

    secret_smtp = params_glue["SMTP_SECRET"]

    key_secret_smtp = get_secret(secret_smtp)

    secret_db = params_glue["DB_SECRET"]

    key_secret_db = get_secret(secret_db)
    
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port":key_secret_sftp["sftp_port"],
        "username":key_secret_sftp["sftp_user"],
        "password":key_secret_sftp["sftp_password"]
    }

    data_connection_smtp = {
        "server": key_secret_smtp["smtp_server"],
        "port": key_secret_smtp["smtp_port"],
        "user": key_secret_smtp["smtp_user"],
        "password": key_secret_smtp["smtp_password"]
    }

    route_general_calendar = key_secret_sftp["route_general_calendar"]
    mail_from = key_secret_smtp["mail_from"]
    mail_to = key_secret_smtp["mail_to"]
    today_date = params_glue["VALUATION_DATE"]
    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc+schema_precia_utils_otc

    client = connect_to_sftp(data_connection_sftp)
    sftp_connect = client.open_sftp()
    file_calendar = FileGeneralCalendarDays()
    df_calendar_days = file_calendar.get_data_file_from_sftp(sftp_connect, route_general_calendar)
    sftp_connect.close()
    client.close()

    date_valuation_df = df_calendar_days.at[0,'valuation_date']
    if today_date != date_valuation_df:
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(data_connection_smtp)
        message = email.create_mail_base(mail_from, mail_to)
        email.send_email(smtp_connection, message)
        logger.error(create_log_msg("La fecha del archivo no coincide con la del día de valoración"))
        raise PlataformError("El archivo no tiene la fecha actual para el día de valoración")
    
    df_calendar_days.fillna("NA", inplace=True)
    db = DataBase(url_db_precia_utils_otc)
    db.disable_previous_info(df_calendar_days)
    db.insert_data(df_calendar_days)
    

if __name__ == "__main__":
    main()
